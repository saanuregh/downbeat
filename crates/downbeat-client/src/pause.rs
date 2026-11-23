//! What one pause holds.

/// What [`ActingClient::pause`](crate::ActingClient::pause) and
/// [`ActingClient::resume`](crate::ActingClient::resume) act on, by name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum PauseKind {
    /// Every run of one job.
    Job,
    /// Every run on one queue.
    Queue,
    /// One schedule's occurrences.
    Schedule,
}

impl PauseKind {
    /// The path segment its endpoints are under: `/v1/jobs/{name}/pause` and so on.
    pub(crate) fn segment(self) -> &'static str {
        match self {
            PauseKind::Job => "jobs",
            PauseKind::Queue => "queues",
            PauseKind::Schedule => "schedules",
        }
    }
}
