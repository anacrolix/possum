use cfg_if::cfg_if;

fn emulate_freebsd_from_env() -> bool {
    use once_cell::sync::OnceCell;
    static CELL: OnceCell<bool> = OnceCell::new();
    *CELL.get_or_init(|| {
        let emulate = !matches!(
            std::env::var("POSSUM_EMULATE_FREEBSD"),
            Err(std::env::VarError::NotPresent)
        );
        if emulate {
            super::error!("emulating freebsd");
        }
        emulate
    })
}

/// FreeBSD doesn't support open file description (range) locking, even in 15.0. We can emulate
/// FreeBSD on platforms that have flock(). Block cloning is probed at runtime everywhere, since
/// FreeBSD 15.0 can clone on ZFS.
pub(crate) fn emulate_freebsd() -> bool {
    cfg_if! {
        if #[cfg(target_os = "freebsd")] {
            true
        } else {
            emulate_freebsd_from_env()
        }
    }
}

/// Whether to use flock() instead of file segment locking. flock() is not available on Windows.
pub(crate) fn flocking() -> bool {
    emulate_freebsd()
}
