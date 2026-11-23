//! The closed sets of words the API answers with: statuses, kinds, reason codes and error codes.
//!
//! Each is written in JSON as the same text as before (`"failed"`, `"not ready"`), and each has an
//! `Unknown` variant that a value added by a newer scheduler decodes as, so an older client still
//! reads the answer. `Unknown` is never sent.

use serde::de::IntoDeserializer;
use serde::{Deserialize, Serialize};

/// Declares a closed set of words: the enum, its text, and its reading from text, where a word this
/// version does not know reads as `Unknown`.
macro_rules! words {
    ($(#[$meta:meta])* $name:ident { $($(#[$variant_meta:meta])* $variant:ident => $text:literal,)* }) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[non_exhaustive]
        pub enum $name {
            $(
                $(#[$variant_meta])*
                #[serde(rename = $text)]
                $variant,
            )*
            /// A word a newer scheduler sent that this version does not know.
            #[default]
            #[serde(other, rename = "unknown")]
            Unknown,
        }

        impl $name {
            /// The word, as the API writes it.
            pub fn as_str(self) -> &'static str {
                match self {
                    $($name::$variant => $text,)*
                    $name::Unknown => "unknown",
                }
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl From<&str> for $name {
            /// Reads the word; one this version does not know is `Unknown`.
            fn from(text: &str) -> Self {
                let text: serde::de::value::StrDeserializer<'_, serde::de::value::Error> = text.into_deserializer();
                $name::deserialize(text).unwrap_or($name::Unknown)
            }
        }
    };
}

words! {
    /// Where a run is: waiting, executing, or how it ended.
    RunStatus {
        Queued => "queued",
        Running => "running",
        Succeeded => "succeeded",
        Skipped => "skipped",
        Failed => "failed",
        Cancelled => "cancelled",
        Expired => "expired",
    }
}

words! {
    /// Why a queued run is not running.
    QueuedBecause {
        NoFreeWorker => "no_free_worker",
        WaitingTurn => "waiting_turn",
        Paused => "paused",
        NeedNotMet => "need_not_met",
        NotReady => "not_ready",
        BackingOff => "backing_off",
        ScheduledLater => "scheduled_later",
    }
}

words! {
    /// Why a run was skipped.
    SkippedBecause {
        JobSkipped => "job_skipped",
        ConditionFalse => "condition_false",
        NeedNotMet => "need_not_met",
        InputUnchanged => "input_unchanged",
        Joined => "joined",
    }
}

words! {
    /// How a run failed.
    FailedBecause {
        Error => "error",
        Panic => "panic",
        Crash => "crash",
        OutOfMemory => "out_of_memory",
        Timeout => "timeout",
        ExitCode => "exit_code",
        Signal => "signal",
        BadInput => "bad_input",
        OutputTooLarge => "output_too_large",
        NeedNotMet => "need_not_met",
        GaveUp => "gave_up",
        WorkerLost => "worker_lost",
        JobNotRegistered => "job_not_registered",
    }
}

words! {
    /// How one call of a run's function ended.
    CallEnded {
        Succeeded => "succeeded",
        Skipped => "skipped",
        NotReady => "not ready",
        Cancelled => "cancelled",
        Interrupted => "interrupted",
        WorkerLost => "worker lost",
        Error => "error",
        Panic => "panic",
        Crash => "crash",
        OutOfMemory => "out_of_memory",
        Timeout => "timeout",
        ExitCode => "exit_code",
        Signal => "signal",
        BadInput => "bad_input",
        OutputTooLarge => "output_too_large",
    }
}

words! {
    /// What a job runs.
    JobKind {
        /// A Rust function, `#[job]`.
        Function => "function",
        /// A program, `#[command]`.
        Command => "command",
    }
}

words! {
    /// Where a backfill is.
    BackfillState {
        Running => "running",
        Done => "done",
        Failed => "failed",
        Cancelled => "cancelled",
    }
}

words! {
    /// What kind of error an API answered with.
    ErrorCode {
        BadRequest => "bad_request",
        Unauthorized => "unauthorized",
        Forbidden => "forbidden",
        NotFound => "not_found",
        MethodNotAllowed => "method_not_allowed",
        Conflict => "conflict",
        TooLarge => "too_large",
        BadInput => "bad_input",
        RateLimited => "rate_limited",
        /// Refused for a reason with no code of its own.
        Refused => "refused",
        Internal => "internal",
        /// The database could not answer just now; try again.
        Database => "database",
        /// A console could not reach the scheduler.
        SchedulerUnreachable => "scheduler_unreachable",
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn each_word_is_written_as_before() {
        assert_eq!(serde_json::to_value(RunStatus::Succeeded).unwrap(), json!("succeeded"));
        assert_eq!(serde_json::to_value(CallEnded::NotReady).unwrap(), json!("not ready"));
        assert_eq!(serde_json::to_value(CallEnded::WorkerLost).unwrap(), json!("worker lost"));
        assert_eq!(serde_json::to_value(FailedBecause::OutOfMemory).unwrap(), json!("out_of_memory"));
        assert_eq!(serde_json::from_value::<ErrorCode>(json!("rate_limited")).unwrap(), ErrorCode::RateLimited);
        assert_eq!(QueuedBecause::from("scheduled_later"), QueuedBecause::ScheduledLater);
        assert_eq!(BackfillState::Done.to_string(), "done");
        assert_eq!(JobKind::from("command").as_str(), "command");
    }

    #[test]
    fn a_word_from_a_newer_scheduler_reads_as_unknown() {
        assert_eq!(serde_json::from_value::<RunStatus>(json!("paused_forever")).unwrap(), RunStatus::Unknown);
        assert_eq!(SkippedBecause::from("a new reason"), SkippedBecause::Unknown);
        assert_eq!(serde_json::from_value::<ErrorCode>(json!("unknown")).unwrap(), ErrorCode::Unknown);
    }
}
