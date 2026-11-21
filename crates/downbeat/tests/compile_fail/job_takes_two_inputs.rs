use downbeat::prelude::*;

#[job]
async fn two_inputs(_run: Run, a: u32, b: u32) -> JobResult<u32> {
    Ok(a + b)
}

fn main() {}
