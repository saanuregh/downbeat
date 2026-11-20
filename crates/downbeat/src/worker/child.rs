//! A worker's handle on one child process: start it, talk to it, replace it.

use std::io;
use std::os::fd::OwnedFd;
use std::process::{Command, Stdio};
use std::time::Duration;

use tokio::net::UnixStream;
use tokio::net::unix::OwnedWriteHalf;
use tokio::sync::mpsc;
use uuid::Uuid;

use super::process::{Launch, Process, Spawner};
use crate::child::{CHILD_FD, ENVIRONMENT};
use crate::protocol::{self, FromChild, ToChild};

/// How long a new child may take to build the definition and say it is ready.
const READY_WITHIN: Duration = Duration::from_secs(60);

pub(crate) struct ChildHandle {
    pub(crate) process: Process,
    writer: OwnedWriteHalf,
    messages: mpsc::UnboundedReceiver<FromChild>,
    /// Runs this child has done, so it is replaced after a set number.
    pub(crate) runs: u32,
    /// A function left tasks running here: the child is replaced after its run.
    pub(crate) spoiled: bool,
}

impl ChildHandle {
    /// Starts a child from the worker's own binary and waits until it is ready.
    pub(crate) async fn start(spawner: &Spawner, environment: Option<&str>) -> io::Result<ChildHandle> {
        let (ours, theirs) = std::os::unix::net::UnixStream::pair()?;
        let mut command = Command::new(std::env::current_exe()?);
        command.env(CHILD_FD, "3").stdin(Stdio::null());
        match environment {
            Some(environment) => command.env(ENVIRONMENT, environment),
            None => command.env_remove(ENVIRONMENT),
        };
        let process = spawner.spawn(Launch { command, socket: Some(OwnedFd::from(theirs)) }).await?;

        ours.set_nonblocking(true)?;
        let (mut reader, writer) = UnixStream::from_std(ours)?.into_split();
        let (sender, messages) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Ok(Some(message)) = protocol::read::<_, FromChild>(&mut reader).await {
                if sender.send(message).is_err() {
                    break;
                }
            }
        });
        let mut child = ChildHandle { process, writer, messages, runs: 0, spoiled: false };
        match tokio::time::timeout(READY_WITHIN, child.next()).await {
            Ok(Some(FromChild::Ready)) => Ok(child),
            Ok(_) => Err(io::Error::other("the child process stopped before it was ready; see its output above")),
            Err(_) => Err(io::Error::other("the child process did not become ready within a minute")),
        }
    }

    pub(crate) async fn send(&mut self, message: &ToChild) -> io::Result<()> {
        protocol::write(&mut self.writer, message).await
    }

    /// The next message, or `None` once the child has closed its end (it exited or crashed).
    pub(crate) async fn next(&mut self) -> Option<FromChild> {
        self.messages.recv().await
    }

    /// The next message about this run, or `None` once the child has closed its end. A message
    /// about any other run is dropped (spec 10.3). Safe to cancel: a message is taken only when it
    /// is returned or dropped.
    pub(crate) async fn next_for(&mut self, run_id: Uuid) -> Option<FromChild> {
        next_about(&mut self.messages, run_id).await
    }
}

/// The next message on `messages` about this run, dropping any about another run.
async fn next_about(messages: &mut mpsc::UnboundedReceiver<FromChild>, run_id: Uuid) -> Option<FromChild> {
    loop {
        let message = messages.recv().await?;
        if message.run_id() == Some(run_id) {
            return Some(message);
        }
        tracing::debug!(%run_id, ?message, "a message from the child about another run was ignored");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_message_about_another_run_is_dropped() {
        let (ours, theirs) = (Uuid::now_v7(), Uuid::now_v7());
        let (sender, mut messages) = mpsc::unbounded_channel();
        sender.send(FromChild::Started { run_id: theirs }).unwrap();
        sender.send(FromChild::Ready).unwrap();
        sender.send(FromChild::Started { run_id: ours }).unwrap();
        drop(sender);

        assert_eq!(next_about(&mut messages, ours).await, Some(FromChild::Started { run_id: ours }));
        assert_eq!(next_about(&mut messages, ours).await, None, "nothing else was about this run");
    }
}
