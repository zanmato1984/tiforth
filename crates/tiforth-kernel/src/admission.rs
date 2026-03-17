use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use crate::error::TiforthError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConsumerKind {
    ProjectionOutput,
    SourceInput,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsumerSpec {
    pub name: String,
    pub kind: ConsumerKind,
    pub spillable: bool,
}

impl ConsumerSpec {
    pub fn new(name: impl Into<String>, kind: ConsumerKind, spillable: bool) -> Self {
        Self {
            name: name.into(),
            kind,
            spillable,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdmissionEvent {
    ConsumerOpened {
        name: String,
        kind: ConsumerKind,
        spillable: bool,
    },
    ReserveAdmitted {
        name: String,
        bytes: usize,
    },
    ReserveDenied {
        name: String,
        bytes: usize,
        limit: usize,
    },
    ConsumerShrunk {
        name: String,
        bytes: usize,
    },
    ConsumerReleased {
        name: String,
        bytes: usize,
    },
}

pub trait AdmissionController: Send + Sync {
    fn open(&self, spec: ConsumerSpec) -> Arc<dyn AdmissionConsumer>;
}

pub trait AdmissionConsumer: Send + Sync {
    fn try_reserve(&self, additional_bytes: usize) -> Result<(), TiforthError>;
    fn shrink(&self, bytes: usize) -> Result<(), TiforthError>;
    fn release(&self) -> Result<(), TiforthError>;
    fn local_track_live_claim(&self, _claim_id: u64) {}
    fn local_untrack_live_claim(&self, _claim_id: u64) {}
}

#[derive(Default)]
pub struct NoopAdmissionController;

#[derive(Default)]
struct NoopAdmissionConsumer {
    live_claims: LiveClaimSet,
}

impl AdmissionController for NoopAdmissionController {
    fn open(&self, _spec: ConsumerSpec) -> Arc<dyn AdmissionConsumer> {
        Arc::new(NoopAdmissionConsumer::default())
    }
}

impl AdmissionConsumer for NoopAdmissionConsumer {
    fn try_reserve(&self, _additional_bytes: usize) -> Result<(), TiforthError> {
        Ok(())
    }

    fn shrink(&self, bytes: usize) -> Result<(), TiforthError> {
        self.live_claims.check_shrink(bytes)
    }

    fn release(&self) -> Result<(), TiforthError> {
        self.live_claims.check_release()
    }

    fn local_track_live_claim(&self, claim_id: u64) {
        self.live_claims.track(claim_id);
    }

    fn local_untrack_live_claim(&self, claim_id: u64) {
        self.live_claims.untrack(claim_id);
    }
}

#[derive(Clone)]
pub struct RecordingAdmissionController {
    state: Arc<ControllerState>,
}

struct ControllerState {
    limit: Option<usize>,
    admitted: Mutex<usize>,
    events: Mutex<Vec<AdmissionEvent>>,
}

impl RecordingAdmissionController {
    pub fn unbounded() -> Self {
        Self {
            state: Arc::new(ControllerState {
                limit: None,
                admitted: Mutex::new(0),
                events: Mutex::new(Vec::new()),
            }),
        }
    }

    pub fn with_limit(limit: usize) -> Self {
        Self {
            state: Arc::new(ControllerState {
                limit: Some(limit),
                admitted: Mutex::new(0),
                events: Mutex::new(Vec::new()),
            }),
        }
    }

    pub fn events(&self) -> Vec<AdmissionEvent> {
        self.state
            .events
            .lock()
            .expect("admission events mutex poisoned")
            .clone()
    }
}

struct RecordingAdmissionConsumer {
    state: Arc<ControllerState>,
    spec: ConsumerSpec,
    current: Mutex<usize>,
    live_claims: LiveClaimSet,
}

#[derive(Default)]
struct LiveClaimSet {
    claim_ids: Mutex<BTreeSet<u64>>,
}

impl LiveClaimSet {
    fn track(&self, claim_id: u64) {
        self.claim_ids
            .lock()
            .expect("live claim mutex poisoned")
            .insert(claim_id);
    }

    fn untrack(&self, claim_id: u64) -> bool {
        let mut claim_ids = self.claim_ids.lock().expect("live claim mutex poisoned");
        claim_ids.remove(&claim_id);
        claim_ids.is_empty()
    }

    fn check_shrink(&self, bytes: usize) -> Result<(), TiforthError> {
        if let Some(claim_id) = self.first_live_claim_id() {
            Err(TiforthError::OwnershipContractViolation {
                detail: format!(
                    "attempted to shrink {bytes} bytes from the consumer for live claim {claim_id} while that claim still referenced it"
                ),
            })
        } else {
            Ok(())
        }
    }

    fn check_release(&self) -> Result<(), TiforthError> {
        if let Some(claim_id) = self.first_live_claim_id() {
            Err(TiforthError::OwnershipContractViolation {
                detail: format!(
                    "attempted to release the consumer for live claim {claim_id} while that claim still referenced it"
                ),
            })
        } else {
            Ok(())
        }
    }

    fn first_live_claim_id(&self) -> Option<u64> {
        self.claim_ids
            .lock()
            .expect("live claim mutex poisoned")
            .iter()
            .next()
            .copied()
    }
}

impl AdmissionController for RecordingAdmissionController {
    fn open(&self, spec: ConsumerSpec) -> Arc<dyn AdmissionConsumer> {
        self.state
            .events
            .lock()
            .expect("admission events mutex poisoned")
            .push(AdmissionEvent::ConsumerOpened {
                name: spec.name.clone(),
                kind: spec.kind.clone(),
                spillable: spec.spillable,
            });

        Arc::new(RecordingAdmissionConsumer {
            state: Arc::clone(&self.state),
            spec,
            current: Mutex::new(0),
            live_claims: LiveClaimSet::default(),
        })
    }
}

impl RecordingAdmissionConsumer {
    fn shrink_unchecked(&self, bytes: usize) {
        let mut current = self
            .current
            .lock()
            .expect("consumer admission mutex poisoned");
        let actual = bytes.min(*current);
        *current -= actual;
        *self
            .state
            .admitted
            .lock()
            .expect("admitted bytes mutex poisoned") -= actual;
        self.state
            .events
            .lock()
            .expect("admission events mutex poisoned")
            .push(AdmissionEvent::ConsumerShrunk {
                name: self.spec.name.clone(),
                bytes: actual,
            });
    }

    fn release_unchecked(&self) {
        let mut current = self
            .current
            .lock()
            .expect("consumer admission mutex poisoned");
        let released = *current;
        *current = 0;
        *self
            .state
            .admitted
            .lock()
            .expect("admitted bytes mutex poisoned") -= released;
        self.state
            .events
            .lock()
            .expect("admission events mutex poisoned")
            .push(AdmissionEvent::ConsumerReleased {
                name: self.spec.name.clone(),
                bytes: released,
            });
    }
}

impl AdmissionConsumer for RecordingAdmissionConsumer {
    fn try_reserve(&self, additional_bytes: usize) -> Result<(), TiforthError> {
        let mut admitted = self
            .state
            .admitted
            .lock()
            .expect("admitted bytes mutex poisoned");
        if let Some(limit) = self.state.limit {
            if *admitted + additional_bytes > limit {
                self.state
                    .events
                    .lock()
                    .expect("admission events mutex poisoned")
                    .push(AdmissionEvent::ReserveDenied {
                        name: self.spec.name.clone(),
                        bytes: additional_bytes,
                        limit,
                    });
                return Err(TiforthError::AdmissionDenied {
                    consumer: self.spec.name.clone(),
                    requested: additional_bytes,
                    limit,
                });
            }
        }

        *admitted += additional_bytes;
        *self
            .current
            .lock()
            .expect("consumer admission mutex poisoned") += additional_bytes;
        self.state
            .events
            .lock()
            .expect("admission events mutex poisoned")
            .push(AdmissionEvent::ReserveAdmitted {
                name: self.spec.name.clone(),
                bytes: additional_bytes,
            });
        Ok(())
    }

    fn shrink(&self, bytes: usize) -> Result<(), TiforthError> {
        self.live_claims.check_shrink(bytes)?;
        self.shrink_unchecked(bytes);
        Ok(())
    }

    fn release(&self) -> Result<(), TiforthError> {
        self.live_claims.check_release()?;
        self.release_unchecked();
        Ok(())
    }

    fn local_track_live_claim(&self, claim_id: u64) {
        self.live_claims.track(claim_id);
    }

    fn local_untrack_live_claim(&self, claim_id: u64) {
        if self.live_claims.untrack(claim_id) {
            self.release_unchecked();
        }
    }
}
