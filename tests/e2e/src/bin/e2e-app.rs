fn main() -> std::process::ExitCode {
    // For the test of the ready limit: a child process that never says it is ready.
    if std::env::var_os("E2E_CHILD_STALLS").is_some() && std::env::var_os("DOWNBEAT_CHILD_FD").is_some() {
        loop {
            std::thread::park();
        }
    }
    downbeat::run(downbeat_e2e::definition)
}
