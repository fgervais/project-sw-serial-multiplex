//! End-to-end smoke test: run the real daemon against a fake serial port
//! (a PTY pair held by the test) and verify that a TCP client and the local
//! PTY client both exchange bytes with the port.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

const TIMEOUT: Duration = Duration::from_secs(5);

struct Daemon(Child);

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Read a file (blocking) on a thread, forwarding chunks over a channel.
fn spawn_reader(mut file: std::fs::File) -> mpsc::Receiver<u8> {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut buf = [0u8; 1024];
        while let Ok(n) = file.read(&mut buf) {
            if n == 0 {
                break;
            }
            for &b in &buf[..n] {
                if tx.send(b).is_err() {
                    return;
                }
            }
        }
    });
    rx
}

fn recv_bytes(rx: &mpsc::Receiver<u8>, n: usize, what: &str) -> Vec<u8> {
    let deadline = Instant::now() + TIMEOUT;
    let mut out = Vec::with_capacity(n);
    while out.len() < n {
        let left = deadline.saturating_duration_since(Instant::now());
        match rx.recv_timeout(left) {
            Ok(b) => out.push(b),
            Err(e) => panic!("timed out reading {what} ({} of {n} bytes): {e}", out.len()),
        }
    }
    out
}

/// Read until the stream ends with `needle`. The daemon's PTY buffers serial
/// output even before minicom attaches, so a freshly opened link sees the
/// backlog first — match on the tail instead of the whole stream.
fn recv_until(rx: &mpsc::Receiver<u8>, needle: &[u8], what: &str) {
    let deadline = Instant::now() + TIMEOUT;
    let mut out = Vec::new();
    while !out.ends_with(needle) {
        let left = deadline.saturating_duration_since(Instant::now());
        match rx.recv_timeout(left) {
            Ok(b) => out.push(b),
            Err(e) => panic!("timed out reading {what}: {e}"),
        }
    }
}

fn wait_for(cond: impl Fn() -> bool, what: &str) {
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline {
        if cond() {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("timed out waiting for {what}");
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Fake serial device: the daemon opens the returned slave path as --port,
/// the test plays the attached hardware on the master side.
///
/// The slave is put in raw mode up front: a fresh PTY defaults to cooked
/// mode, and until the daemon opens the port and applies its own settings,
/// bytes written early would be mangled by the line discipline (e.g. DEL
/// erasing the previous byte).
fn fake_serial_port() -> (PathBuf, std::fs::File) {
    use nix::sys::termios::{SetArg, cfmakeraw, tcgetattr, tcsetattr};

    let fake = nix::pty::openpty(None, None).unwrap();
    let mut tio = tcgetattr(&fake.slave).unwrap();
    cfmakeraw(&mut tio);
    tcsetattr(&fake.slave, SetArg::TCSANOW, &tio).unwrap();
    let slave_path =
        std::fs::read_link(format!("/proc/self/fd/{}", fake.slave.as_raw_fd())).unwrap();
    (slave_path, fake.master.into())
}

/// Spawn the daemon against the given fake serial port.
fn spawn_daemon(slave_path: &Path, data_port: u16, link: &Path, extra_args: &[&str]) -> Daemon {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_sermuxd"));
    cmd.args([
        "--port",
        slave_path.to_str().unwrap(),
        "--bind",
        "127.0.0.1",
        "--data-port",
        &data_port.to_string(),
        "--pty-link",
        link.to_str().unwrap(),
    ])
    .args(extra_args)
    .stdout(Stdio::null())
    .stderr(Stdio::null());
    Daemon(cmd.spawn().unwrap())
}

/// Connect a TCP client, retrying until the listener is up.
fn connect_client(data_port: u16) -> TcpStream {
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline {
        match TcpStream::connect(("127.0.0.1", data_port)) {
            Ok(s) => return s,
            Err(_) => std::thread::sleep(Duration::from_millis(20)),
        }
    }
    panic!("timed out connecting to data port");
}

#[test]
fn tcp_and_pty_clients_share_the_port() {
    let (slave_path, mut serial_master) = fake_serial_port();

    let data_port = free_port();
    let link = std::env::temp_dir().join(format!("sermuxd-test-{}", std::process::id()));
    let _ = std::fs::remove_file(&link);

    let _daemon = spawn_daemon(&slave_path, data_port, &link, &[]);

    // The PTY symlink appears once the daemon is up.
    wait_for(|| link.symlink_metadata().is_ok(), "pty symlink");

    let mut tcp = connect_client(data_port);
    tcp.set_read_timeout(Some(TIMEOUT)).unwrap();

    let serial_rx = spawn_reader(serial_master.try_clone().unwrap());

    // TCP client -> serial port.
    tcp.write_all(b"hello from tcp").unwrap();
    assert_eq!(
        recv_bytes(&serial_rx, b"hello from tcp".len(), "serial port"),
        b"hello from tcp"
    );

    // Serial port -> TCP client.
    serial_master.write_all(b"boot log from serial").unwrap();
    let mut buf = [0u8; 20];
    tcp.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"boot log from serial");

    // The local PTY client, opened through the stable symlink like minicom.
    let mux_fd = nix::fcntl::open(
        Path::new(&link),
        nix::fcntl::OFlag::O_RDWR | nix::fcntl::OFlag::O_NOCTTY,
        nix::sys::stat::Mode::empty(),
    )
    .unwrap();
    let mut mux: std::fs::File = mux_fd.into();
    let mux_rx = spawn_reader(mux.try_clone().unwrap());

    // PTY client -> serial port.
    mux.write_all(b"hello from minicom").unwrap();
    assert_eq!(
        recv_bytes(&serial_rx, b"hello from minicom".len(), "serial port"),
        b"hello from minicom"
    );

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

    let _daemon = spawn_daemon(&slave_path, data_port, &link, &[]);

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

    let _daemon = spawn_daemon(&slave_path, data_port, &link, &["--replay-kb", "4"]);

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

    let link: PathBuf =
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
            "--pty-link",
            link.to_str().unwrap(),
        ])
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
