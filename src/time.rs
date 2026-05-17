pub fn now_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: ts is a valid stack-allocated timespec; CLOCK_REALTIME is always available on Linux.
    let ret = unsafe { libc::clock_gettime(libc::CLOCK_REALTIME, &mut ts) };
    assert_eq!(ret, 0, "clock_gettime failed: {}", std::io::Error::last_os_error());
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_now_ns_increasing() {
        let a = now_ns();
        std::thread::sleep(std::time::Duration::from_millis(1));
        let b = now_ns();
        assert!(b > a);
    }

    #[test]
    fn test_now_ns_reasonable_epoch() {
        // After 2020-01-01
        assert!(now_ns() > 1_577_836_800_000_000_000);
    }
}
