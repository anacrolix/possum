use std::io::Error;
use std::os::fd::AsRawFd;

#[derive(clap::Subcommand)]
enum Commands {
    PunchHole {
        file: String,
        offset: libc::off_t,
        length: libc::off_t,
    },
}

#[derive(clap::Parser)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

fn main() -> anyhow::Result<()> {
    let cli: Cli = clap::Parser::parse();
    use Commands::*;
    match cli.command {
        PunchHole {file, offset, length} => {
            let file = std::fs::OpenOptions::new().write(true).open(file)?;
            let punchhole = libc::fpunchhole_t {
                fp_flags: 0,
                reserved: 0,
                fp_offset: offset,
                fp_length: length,
            };
            let fcntl_res = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_PUNCHHOLE, punchhole) };
            if fcntl_res == -1 {
                eprintln!("{:?}", Error::last_os_error());
            }
        }
    }
    Ok(())
}
