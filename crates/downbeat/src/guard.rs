//! Calling the app's code from the scheduler role: a schedule's input, a day rule, a callback. A
//! panic there must cost that one call, never the scheduler, so each call is guarded.

use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};

use futures::FutureExt;

/// Calls `call`, turning a panic into an error that names `what`.
pub(crate) fn guarded<T>(what: &str, call: impl FnOnce() -> Result<T, String>) -> Result<T, String> {
    catch_unwind(AssertUnwindSafe(call)).unwrap_or_else(|panic| Err(panicked(what, &*panic)))
}

/// Awaits `call`, turning a panic into an error that names `what`.
pub(crate) async fn guarded_async<T>(what: &str, call: impl Future<Output = Result<T, String>>) -> Result<T, String> {
    AssertUnwindSafe(call).catch_unwind().await.unwrap_or_else(|panic| Err(panicked(what, &*panic)))
}

fn panicked(what: &str, panic: &(dyn std::any::Any + Send)) -> String {
    let message = panic
        .downcast_ref::<&str>()
        .map(|text| (*text).to_owned())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "no message".to_owned());
    format!("{what} panicked: {message}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_panic_becomes_an_error() {
        let result: Result<(), String> = guarded("the input of schedule x", || panic!("boom"));
        assert_eq!(result, Err("the input of schedule x panicked: boom".to_owned()));
    }

    #[tokio::test]
    async fn a_panic_in_a_future_becomes_an_error() {
        let result: Result<(), String> = guarded_async("a day rule", async { panic!("bang") }).await;
        assert_eq!(result, Err("a day rule panicked: bang".to_owned()));
    }
}
