//! Offline fixture driver for disposable test mounts. It starts no network
//! clients and only writes `probe-state`/`probe-directory` below its data path.

#[allow(dead_code)]
#[path = "../src/volume.rs"]
mod volume;

use std::io::{self, BufRead, Write};
use std::path::Path;

fn main() -> io::Result<()> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.len() != 3 {
        return Err(io::Error::other(
            "usage: volume_probe MOUNT UUID DATA_DIRECTORY (disposable fixtures only)",
        ));
    }
    let uuid = args[1]
        .to_str()
        .ok_or_else(|| io::Error::other("UUID is not UTF-8"))?;
    let volume =
        volume::ExpectedVolume::prepare(Path::new(&args[0]), uuid, Path::new(&args[2]), &mut None)?;
    println!("ready");
    io::stdout().flush()?;
    for command in io::stdin().lock().lines() {
        let result = match command?.as_str() {
            "check" => volume.check(),
            "write" => write_fixture(),
            "exit" => return Ok(()),
            _ => Err(io::Error::other("unknown fixture operation")),
        };
        match result {
            Ok(()) => println!("ok"),
            Err(error) => println!("error: {error}"),
        }
        io::stdout().flush()?;
    }
    Ok(())
}

fn write_fixture() -> io::Result<()> {
    let mut staged = logex_fs::StagedFile::new_in(Path::new("."), ".probe-state-")?;
    staged.as_file_mut().write_all(b"owned volume fixture")?;
    staged.as_file().sync_all()?;
    staged.persist(Path::new("probe-state"))?;
    std::fs::create_dir_all("probe-directory")
}
