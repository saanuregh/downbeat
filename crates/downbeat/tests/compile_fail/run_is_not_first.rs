use downbeat::prelude::*;

#[job]
async fn takes_input_first(input: u32, _run: Run) -> JobResult<u32> {
    Ok(input)
}

fn main() {}
