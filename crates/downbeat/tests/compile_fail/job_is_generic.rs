use downbeat::prelude::*;

#[job]
async fn generic<T>(_run: Run) -> JobResult<()> {
    Ok(())
}

fn main() {}
