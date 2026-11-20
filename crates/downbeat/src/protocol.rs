//! What crosses the pipe between a worker and a child process.
//!
//! Each message is JSON, framed by its length as four big-endian bytes. Every message about a run
//! carries the run's id, and the worker ignores one that does not belong to the current run.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use uuid::Uuid;

use crate::job::{ProgramSpec, Started};

/// The largest message either side accepts: inputs and outputs are capped well below this.
const MAX_MESSAGE: u32 = 4 * 1024 * 1024;

/// What a run looks like to the child.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct RunInfo {
    pub(crate) run_id: Uuid,
    pub(crate) job: String,
    pub(crate) attempt: u32,
    pub(crate) due_at: chrono::DateTime<chrono::Utc>,
    pub(crate) dry_run: bool,
}

/// The worker asks the child.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "message", rename_all = "snake_case")]
pub(crate) enum ToChild {
    /// Build the run's input and evaluate its conditions. For a linked run, `parents` holds what
    /// each parent handed over, in the link's order; otherwise `input` is the stored input.
    Prepare { run: RunInfo, input: Option<Value>, parents: Option<Vec<Value>> },
    /// Call the function. For a command, answer with the program to start.
    Go { run: RunInfo, input: Value, checkpoint: Option<Value> },
    /// Check what a command's program wrote against the command's output type.
    CheckOutput { run_id: Uuid, job: String, output: Value },
    /// Ask the running function to stop.
    Cancel { run_id: Uuid },
    /// The answer to a `Start` or a `Checkpoint` that asked for one.
    Answer { run_id: Uuid, request: u64, result: Result<Option<Started>, String> },
}

/// The child tells the worker.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "message", rename_all = "snake_case")]
pub(crate) enum FromChild {
    /// The child has built the definition and is ready for runs.
    Ready,
    Prepared {
        run_id: Uuid,
        result: Result<Prepared, String>,
    },
    Program {
        run_id: Uuid,
        result: Result<ProgramSpec, String>,
    },
    /// The function has been called.
    Started {
        run_id: Uuid,
    },
    Progress {
        run_id: Uuid,
        text: String,
    },
    /// A checkpoint; with a request number when the function waits for it to be stored.
    Checkpoint {
        run_id: Uuid,
        state: Value,
        request: Option<u64>,
    },
    /// `run.start(…)`, with the input already checked and hashed.
    Start {
        run_id: Uuid,
        request: u64,
        start: ChildStart,
    },
    Ended {
        run_id: Uuid,
        ending: ChildEnding,
    },
    Checked {
        run_id: Uuid,
        result: Result<Value, String>,
    },
}

/// What preparing a run found.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct Prepared {
    pub(crate) input: Value,
    pub(crate) input_hash: String,
    /// For `one_per`: the field's value.
    pub(crate) turn_value: Option<String>,
    /// The first `only_if` condition that was false.
    pub(crate) failed_condition: Option<String>,
    /// For each need, in order: the hash of the input the needed job must have succeeded for, or
    /// nothing for a `done_within` need.
    pub(crate) need_hashes: Vec<Option<String>>,
}

/// A `run.start(…)` request, checked in the child where the input's type is known.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct ChildStart {
    pub(crate) job: String,
    pub(crate) input: Value,
    pub(crate) input_hash: String,
    pub(crate) turn_value: Option<String>,
    pub(crate) priority: Option<i16>,
    pub(crate) even_if_queued: bool,
    /// The position of this call within the run, so a later attempt does not start it again.
    pub(crate) position: u64,
}

/// How a function call ended, as the child saw it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "ended", content = "detail", rename_all = "snake_case")]
pub(crate) enum ChildEnding {
    Output(Value),
    Skip(String),
    NotReady(String),
    Error(String),
    Panic(String),
    /// The output, in bytes, was over the limit.
    OutputTooLarge(usize),
    /// The input did not decode into the job's type.
    BadInput(String),
}

pub(crate) async fn write<W: AsyncWrite + Unpin, M: Serialize>(writer: &mut W, message: &M) -> std::io::Result<()> {
    let bytes = serde_json::to_vec(message).map_err(std::io::Error::other)?;
    let length = u32::try_from(bytes.len()).map_err(std::io::Error::other)?;
    writer.write_all(&length.to_be_bytes()).await?;
    writer.write_all(&bytes).await?;
    writer.flush().await
}

/// The next message, or `None` when the other side has closed the pipe.
pub(crate) async fn read<R: AsyncRead + Unpin, M: for<'de> Deserialize<'de>>(
    reader: &mut R,
) -> std::io::Result<Option<M>> {
    let mut length = [0_u8; 4];
    match reader.read_exact(&mut length).await {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error),
    }
    let length = u32::from_be_bytes(length);
    if length > MAX_MESSAGE {
        return Err(std::io::Error::other(format!("a message of {length} bytes is over the limit")));
    }
    let mut bytes = vec![0; length as usize];
    reader.read_exact(&mut bytes).await?;
    serde_json::from_slice(&bytes).map(Some).map_err(std::io::Error::other)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn messages_round_trip_through_the_framing() {
        let (mut a, mut b) = tokio::io::duplex(1024);
        let message = FromChild::Progress { run_id: Uuid::nil(), text: "41 of 200".into() };
        write(&mut a, &message).await.unwrap();
        drop(a);
        assert_eq!(read::<_, FromChild>(&mut b).await.unwrap(), Some(message));
        assert_eq!(read::<_, FromChild>(&mut b).await.unwrap(), None);
    }
}
