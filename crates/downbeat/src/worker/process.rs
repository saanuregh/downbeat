//! Starting and stopping processes: job children and command programs.
//!
//! Every process the worker starts is put in its own process group, is marked as the kernel's
//! preferred victim when memory runs out, and gets a parent-death signal, so the operating system
//! kills it the moment the worker dies. That signal is tied to the thread that started the process,
//! so every process is started from one thread that lives as long as the worker.

#![allow(unsafe_code)]

use std::io;
use std::os::fd::{AsRawFd, OwnedFd};
use std::os::unix::process::CommandExt;
use std::process::{Child, Command, ExitStatus};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};

use tokio::sync::oneshot;

/// What to start.
pub(crate) struct Launch {
    pub(crate) command: Command,
    /// A socket end to place on descriptor 3 in the new process.
    pub(crate) socket: Option<OwnedFd>,
}

type Request = (Launch, oneshot::Sender<io::Result<Child>>);

/// Starts processes from one dedicated thread.
#[derive(Clone)]
pub(crate) struct Spawner {
    requests: mpsc::Sender<Request>,
}

impl Spawner {
    pub(crate) fn new() -> Spawner {
        let (requests, receiver) = mpsc::channel::<Request>();
        std::thread::Builder::new()
            .name("downbeat-spawner".into())
            .spawn(move || {
                for (launch, answer) in receiver {
                    let _ = answer.send(spawn(launch));
                }
            })
            .expect("the spawner thread starts");
        Spawner { requests }
    }

    pub(crate) async fn spawn(&self, launch: Launch) -> io::Result<Process> {
        let (answer, receiver) = oneshot::channel();
        self.requests.send((launch, answer)).map_err(|_| io::Error::other("the spawner thread has stopped"))?;
        let child = receiver.await.map_err(|_| io::Error::other("the spawner thread has stopped"))??;
        Ok(Process::watch(child))
    }
}

fn spawn(mut launch: Launch) -> io::Result<Child> {
    let parent = std::process::id();
    let socket = launch.socket.as_ref().map(AsRawFd::as_raw_fd);
    // SAFETY: the closure runs in the forked child before exec and only makes async-signal-safe
    // calls (dup2, fcntl, setpgid, prctl, getppid, open, write, close, _exit) on values copied in.
    unsafe {
        launch.command.pre_exec(move || {
            if let Some(fd) = socket {
                if fd == 3 {
                    if libc::fcntl(3, libc::F_SETFD, 0) == -1 {
                        return Err(io::Error::last_os_error());
                    }
                } else if libc::dup2(fd, 3) == -1 {
                    return Err(io::Error::last_os_error());
                }
            }
            if libc::setpgid(0, 0) == -1 {
                return Err(io::Error::last_os_error());
            }
            if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) == -1 {
                return Err(io::Error::last_os_error());
            }
            // The worker may have died between the fork and the call above.
            if u32::try_from(libc::getppid()).unwrap_or(0) != parent {
                libc::_exit(1);
            }
            let path = c"/proc/self/oom_score_adj";
            let file = libc::open(path.as_ptr(), libc::O_WRONLY);
            if file >= 0 {
                let preferred_victim = b"1000";
                libc::write(file, preferred_victim.as_ptr().cast(), preferred_victim.len());
                libc::close(file);
            }
            Ok(())
        });
    }
    let child = launch.command.spawn();
    drop(launch.socket);
    child
}

/// A started process: its id, and its exit once it has happened.
pub(crate) struct Process {
    pid: i32,
    exited: Arc<AtomicBool>,
    exit: oneshot::Receiver<io::Result<ExitStatus>>,
    status: Option<ExitStatus>,
    pub(crate) child_pipes: Pipes,
}

/// The pipes of a process started with piped standard streams.
#[derive(Default)]
pub(crate) struct Pipes {
    pub(crate) stdin: Option<std::process::ChildStdin>,
    pub(crate) stderr: Option<std::process::ChildStderr>,
}

impl Process {
    fn watch(mut child: Child) -> Process {
        let pid = i32::try_from(child.id()).expect("a process id fits in i32");
        let child_pipes = Pipes { stdin: child.stdin.take(), stderr: child.stderr.take() };
        let exited = Arc::new(AtomicBool::new(false));
        let (sender, exit) = oneshot::channel();
        let flag = Arc::clone(&exited);
        std::thread::Builder::new()
            .name(format!("downbeat-wait-{pid}"))
            .spawn(move || {
                let status = child.wait();
                flag.store(true, Ordering::SeqCst);
                let _ = sender.send(status);
            })
            .expect("a waiting thread starts");
        Process { pid, exited, exit, status: None, child_pipes }
    }

    /// Waits for the process to exit. Returns at once if it already has. Safe to cancel, so it
    /// can sit in a `select!` beside other branches.
    pub(crate) async fn wait(&mut self) -> io::Result<ExitStatus> {
        if let Some(status) = self.status {
            return Ok(status);
        }
        let status = (&mut self.exit).await.unwrap_or_else(|_| Err(io::Error::other("the waiting thread stopped")))?;
        self.status = Some(status);
        Ok(status)
    }

    /// Sends a signal to the whole process group, unless the process has already exited.
    pub(crate) fn signal_group(&self, signal: i32) {
        if !self.exited.load(Ordering::SeqCst) {
            // SAFETY: killpg only sends a signal; the group id is this process's own, which it
            // still holds because it has not been reaped.
            unsafe {
                libc::killpg(self.pid, signal);
            }
        }
    }

    pub(crate) fn kill(&self) {
        self.signal_group(libc::SIGKILL);
    }

    pub(crate) fn has_exited(&self) -> bool {
        self.exited.load(Ordering::SeqCst)
    }

    /// The process's resident memory, in bytes, from `/proc`.
    pub(crate) fn resident_bytes(&self) -> Option<u64> {
        let statm = std::fs::read_to_string(format!("/proc/{}/statm", self.pid)).ok()?;
        let pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
        // SAFETY: sysconf reads a constant of the running system.
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        Some(pages * u64::try_from(page_size).unwrap_or(4096))
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        self.kill();
    }
}

/// How a process ended, in the words the engine records.
pub(crate) fn describe_exit(status: ExitStatus) -> (Option<i32>, Option<i32>) {
    use std::os::unix::process::ExitStatusExt;
    (status.code(), status.signal())
}
