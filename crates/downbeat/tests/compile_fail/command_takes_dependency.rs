use downbeat::prelude::*;

struct Pool;

#[command]
fn lists_files(_pool: &Pool) -> Program<()> {
    Program::new("/bin/ls")
}

fn main() {}
