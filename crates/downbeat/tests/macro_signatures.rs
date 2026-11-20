//! The macros refuse a job or command whose signature the engine cannot call, at compile time.

#[test]
fn bad_job_signatures_do_not_compile() {
    trybuild::TestCases::new().compile_fail("tests/compile_fail/*.rs");
}
