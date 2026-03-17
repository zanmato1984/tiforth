use crate::admission::{AdmissionEvent, RecordingAdmissionController};
use crate::handoff::RuntimeEvent;

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
}
