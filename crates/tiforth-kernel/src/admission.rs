use std::sync::{Arc, Mutex};

use crate::error::TiforthError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConsumerKind {
    ProjectionOutput,
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
    Open {
        name: String,
        kind: ConsumerKind,
        spillable: bool,
    },
    TryReserve {
        name: String,
        bytes: usize,
    },
    Shrink {
        name: String,
        bytes: usize,
    },
    Release {
        name: String,
        bytes: usize,
    },
}

pub trait AdmissionController: Send + Sync {
    fn open(&self, spec: ConsumerSpec) -> Arc<dyn AdmissionConsumer>;
}

pub trait AdmissionConsumer: Send + Sync {
    fn try_reserve(&self, additional_bytes: usize) -> Result<(), TiforthError>;
    fn shrink(&self, bytes: usize);
    fn release(&self);
}

#[derive(Default)]
pub struct NoopAdmissionController;

struct NoopAdmissionConsumer;

impl AdmissionController for NoopAdmissionController {
    fn open(&self, _spec: ConsumerSpec) -> Arc<dyn AdmissionConsumer> {
        Arc::new(NoopAdmissionConsumer)
    }
}

impl AdmissionConsumer for NoopAdmissionConsumer {
    fn try_reserve(&self, _additional_bytes: usize) -> Result<(), TiforthError> {
        Ok(())
    }

    fn shrink(&self, _bytes: usize) {}

    fn release(&self) {}
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
}

impl AdmissionController for RecordingAdmissionController {
    fn open(&self, spec: ConsumerSpec) -> Arc<dyn AdmissionConsumer> {
        self.state
            .events
            .lock()
            .expect("admission events mutex poisoned")
            .push(AdmissionEvent::Open {
                name: spec.name.clone(),
                kind: spec.kind.clone(),
                spillable: spec.spillable,
            });

        Arc::new(RecordingAdmissionConsumer {
            state: Arc::clone(&self.state),
            spec,
            current: Mutex::new(0),
        })
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
            .push(AdmissionEvent::TryReserve {
                name: self.spec.name.clone(),
                bytes: additional_bytes,
            });
        Ok(())
    }

    fn shrink(&self, bytes: usize) {
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
            .push(AdmissionEvent::Shrink {
                name: self.spec.name.clone(),
                bytes: actual,
            });
    }

    fn release(&self) {
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
            .push(AdmissionEvent::Release {
                name: self.spec.name.clone(),
                bytes: released,
            });
    }
}
