//! Attributes on a job function stay on the function, docs go on the handle, and `cfg` removes
//! every generated item.

use downbeat::prelude::*;

/// Doubles a number.
#[job]
#[inline]
#[must_use = "a job's result is its output"]
async fn double(_run: Run, value: u32) -> JobResult<u32> {
    Ok(value * 2)
}

/// Replaced by `double`.
#[job]
#[deprecated = "use double"]
async fn old_double(_run: Run, value: u32) -> JobResult<u32> {
    Ok(value * 2)
}

#[tokio::test]
#[expect(deprecated, reason = "the handle carries the deprecation, so naming it warns")]
async fn a_deprecated_job_warns_where_its_handle_is_used() {
    assert_eq!(old_double.test(Run::for_test(), 2).await, Outcome::Succeeded(4));
}

// `#[cfg]` after `#[job]`, so the macro sees it rather than the compiler removing the function first.
#[job]
#[cfg(any())]
async fn never_compiled(_run: Run) -> JobResult<()> {
    compile_error!("a job behind a false cfg is removed with all its generated items")
}

#[tokio::test]
async fn attributes_stay_on_the_function() {
    let outcome = double.test(Run::for_test(), 21).await;
    assert_eq!(outcome, Outcome::Succeeded(42));
}

#[test]
fn a_disabled_job_is_not_registered() {
    let names: Vec<_> = downbeat::__private::inventory::iter::<downbeat::__private::Registered>
        .into_iter()
        .map(|registered| registered.name)
        .collect();
    assert!(names.contains(&"double"));
    assert!(!names.contains(&"never_compiled"));
}
