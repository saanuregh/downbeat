//! A worker's handle on one child process: start it, talk to it, replace it.

use std::io;
use std::os::fd::OwnedFd;
use std::process::{Command, Stdio};
use std::time::Duration;

use tokio::net::UnixStream;
use tokio::net::unix::OwnedWriteHalf;
use tokio::sync::mpsc;

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
}
