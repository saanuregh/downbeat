use downbeat::prelude::*;

#[job]
fn not_async(_run: Run) -> JobResult<()> {
    Ok(())
}

fn main() {}
