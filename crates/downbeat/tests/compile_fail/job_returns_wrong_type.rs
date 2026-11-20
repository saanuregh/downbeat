use downbeat::prelude::*;

#[job]
async fn wrong_return(_run: Run) -> Option<u32> {
    Some(1)
}

fn main() {}
