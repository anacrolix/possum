pub(crate) mod sync;

#[cfg(not(feature = "shuttle"))]
pub use std::thread;

// This isn't available in loom or shuttle yet. Unfortunately for shuttle it means threads are
// spawned outside its control, and it doesn't work.
#[cfg(feature = "shuttle")]
pub use shuttle::thread;

#[cfg(not(feature = "shuttle"))]
pub(crate) fn run_blocking<F, R>(f: F) -> R
where
    F: FnOnce() -> R + Send,
    R: Send,
{
    if false {
        let (sender, receiver) = std::sync::mpsc::channel();
        let tx_thread = std::thread::scope(|scope| {
            scope.spawn(|| {
                let res = f();
                sender.send(res).unwrap();
            });
            receiver.recv().unwrap()
        });
        tx_thread
    } else {
        f()
    }
}

#[cfg(feature = "shuttle")]
pub(crate) fn run_blocking<F, R>(f: F) -> R
    where
        F: FnOnce() -> R + Send,
        R: Send,
{
    use std::sync::mpsc;
    let (sender, receiver) = mpsc::channel();
    let tx_thread = std::thread::scope(|scope| {
        scope.spawn(||{
            let res = f();
            sender.send(res).unwrap();
        });
        loop {
            shuttle::thread::yield_now();
            match receiver.try_recv() {
                Err(mpsc::TryRecvError::Empty) => continue,
                default => return default.unwrap()
            }
        }
    });
    tx_thread
}
