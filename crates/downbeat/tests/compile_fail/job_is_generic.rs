use downbeat::prelude::*;

#[job]
async fn generic<T>(_run: Run) -> Result<()> {
    Ok(())
}

fn main() {}
