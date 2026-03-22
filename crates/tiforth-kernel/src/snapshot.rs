use crate::admission::{AdmissionEvent, ConsumerKind, RecordingAdmissionController};
use crate::runtime::{BatchOrigin, RuntimeEvent};
use serde::Serialize;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LocalExecutionSnapshot {
    pub admission_events: Vec<AdmissionEvent>,
    pub runtime_events: Vec<RuntimeEvent>,
}

impl LocalExecutionSnapshot {
    pub fn new(admission_events: Vec<AdmissionEvent>, runtime_events: Vec<RuntimeEvent>) -> Self {
        Self {
            admission_events,
            runtime_events,
        }
    }

    pub fn capture(
        admission: &RecordingAdmissionController,
        runtime_events: Vec<RuntimeEvent>,
    ) -> Self {
        Self::new(admission.events(), runtime_events)
    }

    pub fn to_fixture(&self) -> LocalExecutionFixture {
        LocalExecutionFixture {
            admission_events: self
                .admission_events
                .iter()
                .map(AdmissionFixtureEvent::from)
                .collect(),
            runtime_events: self
                .runtime_events
                .iter()
                .map(RuntimeFixtureEvent::from)
                .collect(),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct LocalExecutionFixture {
    pub admission_events: Vec<AdmissionFixtureEvent>,
    pub runtime_events: Vec<RuntimeFixtureEvent>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct AdmissionFixtureEvent {
    pub event: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spillable: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
}

impl AdmissionFixtureEvent {
    pub fn consumer_opened(
        name: impl Into<String>,
        kind: impl Into<String>,
        spillable: bool,
    ) -> Self {
        Self {
            event: "consumer_opened".into(),
            name: name.into(),
            kind: Some(kind.into()),
            spillable: Some(spillable),
            bytes: None,
            limit: None,
        }
    }

    pub fn reserve_admitted(name: impl Into<String>, bytes: usize) -> Self {
        Self {
            event: "reserve_admitted".into(),
            name: name.into(),
            kind: None,
            spillable: None,
            bytes: Some(bytes),
            limit: None,
        }
    }

    pub fn reserve_denied(name: impl Into<String>, bytes: usize, limit: usize) -> Self {
        Self {
            event: "reserve_denied".into(),
            name: name.into(),
            kind: None,
            spillable: None,
            bytes: Some(bytes),
            limit: Some(limit),
        }
    }

    pub fn consumer_shrunk(name: impl Into<String>, bytes: usize) -> Self {
        Self {
            event: "consumer_shrunk".into(),
            name: name.into(),
            kind: None,
            spillable: None,
            bytes: Some(bytes),
            limit: None,
        }
    }

    pub fn consumer_released(name: impl Into<String>, bytes: usize) -> Self {
        Self {
            event: "consumer_released".into(),
            name: name.into(),
            kind: None,
            spillable: None,
            bytes: Some(bytes),
            limit: None,
        }
    }
}

impl From<&AdmissionEvent> for AdmissionFixtureEvent {
    fn from(value: &AdmissionEvent) -> Self {
        match value {
            AdmissionEvent::ConsumerOpened {
                name,
                kind,
                spillable,
            } => Self::consumer_opened(name.clone(), consumer_kind_name(kind), *spillable),
            AdmissionEvent::ReserveAdmitted { name, bytes } => {
                Self::reserve_admitted(name.clone(), *bytes)
            }
            AdmissionEvent::ReserveDenied { name, bytes, limit } => {
                Self::reserve_denied(name.clone(), *bytes, *limit)
            }
            AdmissionEvent::ConsumerShrunk { name, bytes } => {
                Self::consumer_shrunk(name.clone(), *bytes)
            }
            AdmissionEvent::ConsumerReleased { name, bytes } => {
                Self::consumer_released(name.clone(), *bytes)
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RuntimeFixtureEvent {
    pub event: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin: Option<FixtureBatchOrigin>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to_operator: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub claim_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl RuntimeFixtureEvent {
    pub fn batch_emitted(batch_id: u64, origin: FixtureBatchOrigin, claim_count: usize) -> Self {
        Self {
            event: "batch_emitted".into(),
            batch_id: Some(batch_id),
            origin: Some(origin),
            to_operator: None,
            claim_count: Some(claim_count),
            message: None,
        }
    }

    pub fn batch_handed_off(
        batch_id: u64,
        to_operator: impl Into<String>,
        claim_count: usize,
    ) -> Self {
        Self {
            event: "batch_handed_off".into(),
            batch_id: Some(batch_id),
            origin: None,
            to_operator: Some(to_operator.into()),
            claim_count: Some(claim_count),
            message: None,
        }
    }

    pub fn batch_released(batch_id: u64, origin: FixtureBatchOrigin, claim_count: usize) -> Self {
        Self {
            event: "batch_released".into(),
            batch_id: Some(batch_id),
            origin: Some(origin),
            to_operator: None,
            claim_count: Some(claim_count),
            message: None,
        }
    }

    pub fn finished() -> Self {
        Self {
            event: "finished".into(),
            batch_id: None,
            origin: None,
            to_operator: None,
            claim_count: None,
            message: None,
        }
    }

    pub fn cancelled() -> Self {
        Self {
            event: "cancelled".into(),
            batch_id: None,
            origin: None,
            to_operator: None,
            claim_count: None,
            message: None,
        }
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self {
            event: "error".into(),
            batch_id: None,
            origin: None,
            to_operator: None,
            claim_count: None,
            message: Some(message.into()),
        }
    }
}

impl From<&RuntimeEvent> for RuntimeFixtureEvent {
    fn from(value: &RuntimeEvent) -> Self {
        match value {
            RuntimeEvent::BatchEmitted {
                batch_id,
                origin,
                claim_count,
            } => Self::batch_emitted(*batch_id, FixtureBatchOrigin::from(origin), *claim_count),
            RuntimeEvent::BatchHandedOff {
                batch_id,
                to_operator,
                claim_count,
            } => Self::batch_handed_off(*batch_id, to_operator.clone(), *claim_count),
            RuntimeEvent::BatchReleased {
                batch_id,
                origin,
                claim_count,
            } => Self::batch_released(*batch_id, FixtureBatchOrigin::from(origin), *claim_count),
            RuntimeEvent::TerminalFinished => Self::finished(),
            RuntimeEvent::TerminalCancelled => Self::cancelled(),
            RuntimeEvent::TerminalError { message } => Self::error(message.clone()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct FixtureBatchOrigin {
    pub query: String,
    pub stage: String,
    pub operator: String,
}

impl FixtureBatchOrigin {
    pub fn new(
        query: impl Into<String>,
        stage: impl Into<String>,
        operator: impl Into<String>,
    ) -> Self {
        Self {
            query: query.into(),
            stage: stage.into(),
            operator: operator.into(),
        }
    }

    pub fn local(operator: impl Into<String>) -> Self {
        let operator = operator.into();
        Self::new("local-query", operator.clone(), operator)
    }
}

impl From<&BatchOrigin> for FixtureBatchOrigin {
    fn from(value: &BatchOrigin) -> Self {
        Self::new(
            value.query.clone(),
            value.stage.clone(),
            value.operator.clone(),
        )
    }
}

fn consumer_kind_name(kind: &ConsumerKind) -> &'static str {
    match kind {
        ConsumerKind::ProjectionOutput => "projection_output",
        ConsumerKind::FilterOutput => "filter_output",
        ConsumerKind::SourceInput => "source_input",
    }
}
