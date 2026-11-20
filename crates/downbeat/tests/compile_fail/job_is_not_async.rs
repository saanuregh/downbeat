use downbeat::prelude::*;

#[job]
fn not_async(_run: Run) -> Result<()> {
    Ok(())
}

fn main() {}
