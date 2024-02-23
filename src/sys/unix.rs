use std::path::Path;

pub(crate) use nix::errno::errno;

pub fn path_disk_allocation(path: &Path) -> std::io::Result<u64> {
    let metadata = std::fs::metadata(path)?;
    use std::os::unix::fs::MetadataExt;
    Ok(metadata.blocks() * 512)
}
