use cfg_if::cfg_if;

pub(crate) fn emulate_freebsd() -> bool {
    cfg_if! {
        if #[cfg(target_os = "freebsd")] {
            true
        } else {
            use once_cell::sync::OnceCell;
            static CELL: OnceCell<bool> = OnceCell::new();
            *CELL.get_or_init(|| {
                let emulate =                 !matches!(
                    std::env::var("POSSUM_EMULATE_FREEBSD"),
                    Err(std::env::VarError::NotPresent)
                );
                if emulate {
                    super::error!("emulating freebsd");
                }
                emulate
            })
        }
    }
}

pub(crate) fn flocking() -> bool {
    emulate_freebsd()
}
