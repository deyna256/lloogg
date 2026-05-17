use std::fs;
use std::process::Command;
use std::thread;
use std::time::Duration;

#[test]
fn binary_stays_alive_after_startup() {
    let tmpdir = tempfile::tempdir().expect("tempdir");
    let config_path = tmpdir.path().join("lloogg.test.toml");
    let aof_path = tmpdir.path().join("test.aof");
    let snapshot_path = tmpdir.path().join("snapshots");

    fs::create_dir_all(&snapshot_path).expect("create snapshot dir");
    fs::write(
        &config_path,
        format!(
            concat!(
                "max_records = 100\n",
                "ttl_seconds = 3600\n",
                "aof_path = {:?}\n",
                "snapshot_path = {:?}\n",
                "snapshot_interval = 3600\n",
                "listen_port = 0\n",
                "memory_pool_slots = 1000\n",
                "aof_fsync = \"no\"\n",
            ),
            aof_path,
            snapshot_path,
        ),
    )
    .expect("write config");

    let mut child = Command::new(env!("CARGO_BIN_EXE_lloogg"))
        .arg(&config_path)
        .spawn()
        .expect("spawn lloogg");

    thread::sleep(Duration::from_millis(150));

    let status = child.try_wait().expect("query child status");
    if status.is_none() {
        child.kill().expect("kill child");
        let _ = child.wait();
    }

    assert!(
        status.is_none(),
        "binary exited during startup with status: {status:?}"
    );
}
