use downbeat::prelude::*;

#[command]
async fn lists_files() -> Program<()> {
    Program::new("/bin/ls")
}

fn main() {}
