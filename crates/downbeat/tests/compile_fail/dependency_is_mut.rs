use downbeat::prelude::*;

struct Pool;

#[job]
async fn takes_mut(_run: Run, _pool: &mut Pool) -> Result<()> {
    Ok(())
}

fn main() {}
