use downbeat::prelude::*;

#[command]
fn copies(from: String, to: String) -> Program<()> {
    Program::new("/bin/cp").arg(from).arg(to)
}

fn main() {}
