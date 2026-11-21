use downbeat::prelude::*;

struct Pool;

#[job]
async fn input_last(_run: Run, _pool: &Pool, n: u32) -> JobResult<u32> {
    Ok(n)
}

fn main() {}
