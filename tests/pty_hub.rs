//! End-to-end smoke test: run the real daemon against a fake serial port
//! (a PTY pair held by the test) and verify that a TCP client and the local
//! PTY client both exchange bytes with the port.

use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Duration;

mod common;
use common::{
    TIMEOUT, connect_client, fake_serial_port, free_port, recv_bytes, recv_expect_none, recv_until,
    spawn_daemon, spawn_reader, wait_for, wait_for_default_owner,
};

#[test]
fn tcp_and_pty_clients_share_the_port() {
    let (slave_path, mut serial_master) = fake_serial_port();

    let data_port = free_port();
    let link = std::env::temp_dir().join(format!("sermuxd-test-{}", std::process::id()));
    let _ = std::fs::remove_file(&link);

    let (_daemon, control_port) = spawn_daemon(&slave_path, data_port, &link, &[]);

    // The PTY symlink appears once the daemon is up; arbitration is armed
    // once the PTY registers as the default owner.
    wait_for(|| link.symlink_metadata().is_ok(), "pty symlink");
    wait_for_default_owner(control_port);

    let mut tcp = connect_client(data_port);
    tcp.set_read_timeout(Some(TIMEOUT)).unwrap();

    let serial_rx = spawn_reader(serial_master.try_clone().unwrap());

    // The local PTY client, opened through the stable symlink like minicom.
    let mux_fd = nix::fcntl::open(
        Path::new(&link),
        nix::fcntl::OFlag::O_RDWR | nix::fcntl::OFlag::O_NOCTTY,
        nix::sys::stat::Mode::empty(),
    )
    .unwrap();
    let mut mux: std::fs::File = mux_fd.into();
    let mux_rx = spawn_reader(mux.try_clone().unwrap());

    // TCP client -> serial port. Without a token the write is dropped (the
    // PTY is the default TX owner).
    tcp.write_all(b"hello from tcp").unwrap();
    recv_expect_none(&serial_rx, "unowned tcp write");

    // PTY client (default TX owner) -> serial port.
    mux.write_all(b"hello from minicom").unwrap();
    assert_eq!(
        recv_bytes(&serial_rx, b"hello from minicom".len(), "serial port"),
        b"hello from minicom"
    );

    // Serial port -> TCP client.
    serial_master.write_all(b"boot log from serial").unwrap();
    let mut buf = [0u8; 20];
    tcp.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"boot log from serial");

    // Serial port -> PTY client.
    serial_master.write_all(b"for minicom").unwrap();
    recv_until(&mux_rx, b"for minicom", "pty client");
}

#[test]
fn new_tcp_client_receives_replay_history() {
    let (slave_path, mut serial_master) = fake_serial_port();

    let data_port = free_port();
    let link = std::env::temp_dir().join(format!("sermuxd-test-replay-{}", std::process::id()));
    let _ = std::fs::remove_file(&link);

    let (_daemon, _control) = spawn_daemon(&slave_path, data_port, &link, &[]);

    // Serial output while NO client is connected lands in the replay buffer.
    serial_master.write_all(b"early boot log\n").unwrap();
    wait_for(|| link.symlink_metadata().is_ok(), "pty symlink");
    std::thread::sleep(Duration::from_millis(300)); // let it reach the buffer

    let mut tcp = connect_client(data_port);
    tcp.set_read_timeout(Some(TIMEOUT)).unwrap();

    // The replayed history comes first, then the live stream seamlessly.
    let mut buf = [0u8; 15];
    tcp.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"early boot log\n");

    serial_master.write_all(b"live").unwrap();
    let mut buf = [0u8; 4];
    tcp.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"live");
}

#[test]
fn replay_buffer_rolls_over_at_its_cap() {
    let (slave_path, mut serial_master) = fake_serial_port();

    let data_port = free_port();
    let link = std::env::temp_dir().join(format!("sermuxd-test-cap-{}", std::process::id()));
    let _ = std::fs::remove_file(&link);

    let (_daemon, _control) = spawn_daemon(&slave_path, data_port, &link, &["--replay-kb", "4"]);

    // 8 KiB of history into a 4 KiB buffer: only a suffix survives.
    let stream: Vec<u8> = (0..8192u32).map(|i| (i % 251) as u8).collect();
    serial_master.write_all(&stream).unwrap();
    wait_for(|| link.symlink_metadata().is_ok(), "pty symlink");
    std::thread::sleep(Duration::from_millis(300));

    let mut tcp = connect_client(data_port);
    tcp.set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();

    // Collect the replay (read until the live stream goes silent).
    let mut replayed = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        match tcp.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => replayed.extend_from_slice(&buf[..n]),
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                break;
            }
            Err(e) => panic!("read error: {e}"),
        }
    }

    // A non-empty suffix of the stream, within the 4 KiB cap.
    assert!(!replayed.is_empty(), "expected some replay history");
    assert!(
        replayed.len() <= 4096,
        "replay exceeded cap: {} bytes",
        replayed.len()
    );
    assert_eq!(
        replayed,
        stream[stream.len() - replayed.len()..],
        "replay is not a suffix of the stream"
    );
}

#[test]
fn refuses_to_overwrite_a_real_file() {
    let fake = nix::pty::openpty(None, None).unwrap();
    let slave_path =
        std::fs::read_link(format!("/proc/self/fd/{}", fake.slave.as_raw_fd())).unwrap();

    let link: std::path::PathBuf =
        std::env::temp_dir().join(format!("sermuxd-test-busy-{}", std::process::id()));
    std::fs::write(&link, b"i am a real file").unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_sermuxd"))
        .args([
            "--port",
            slave_path.to_str().unwrap(),
            "--bind",
            "127.0.0.1",
            "--data-port",
            &free_port().to_string(),
            "--control-port",
            &free_port().to_string(),
            "--pty-link",
            link.to_str().unwrap(),
        ])
        .stdout(Stdio::null())
        .output()
        .unwrap();

    assert!(!output.status.success(), "daemon should refuse to start");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("is not a symlink"),
        "unexpected stderr: {stderr}"
    );
    // The real file must survive.
    assert_eq!(std::fs::read(&link).unwrap(), b"i am a real file");
    std::fs::remove_file(&link).unwrap();
}
