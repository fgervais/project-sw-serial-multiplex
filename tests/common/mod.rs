//! Shared helpers for the end-to-end daemon tests. Each test binary compiles
//! its own copy of this module; not every helper is used in every binary.
#![allow(dead_code)]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

pub const TIMEOUT: Duration = Duration::from_secs(5);

pub struct Daemon(Child);

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Read a file (blocking) on a thread, forwarding chunks over a channel.
pub fn spawn_reader(mut file: std::fs::File) -> mpsc::Receiver<u8> {
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

pub fn recv_bytes(rx: &mpsc::Receiver<u8>, n: usize, what: &str) -> Vec<u8> {
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
pub fn recv_until(rx: &mpsc::Receiver<u8>, needle: &[u8], what: &str) {
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

/// Assert that nothing arrives for a short while (negative assertion, e.g.
/// bytes that must have been dropped).
pub fn recv_expect_none(rx: &mpsc::Receiver<u8>, what: &str) {
    std::thread::sleep(Duration::from_millis(300));
    let mut got = Vec::new();
    while let Ok(b) = rx.try_recv() {
        got.push(b);
    }
    assert!(
        got.is_empty(),
        "expected nothing on {what}, got {} bytes: {got:?}",
        got.len()
    );
}

pub fn wait_for(mut cond: impl FnMut() -> bool, what: &str) {
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline {
        if cond() {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("timed out waiting for {what}");
}

pub fn free_port() -> u16 {
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
pub fn fake_serial_port() -> (PathBuf, std::fs::File) {
    use nix::sys::termios::{SetArg, cfmakeraw, tcgetattr, tcsetattr};

    let fake = nix::pty::openpty(None, None).unwrap();
    let mut tio = tcgetattr(&fake.slave).unwrap();
    cfmakeraw(&mut tio);
    tcsetattr(&fake.slave, SetArg::TCSANOW, &tio).unwrap();
    let slave_path =
        std::fs::read_link(format!("/proc/self/fd/{}", fake.slave.as_raw_fd())).unwrap();
    (slave_path, fake.master.into())
}

/// Spawn the daemon against the given fake serial port, on fresh data and
/// control ports. Returns the daemon and its control port.
pub fn spawn_daemon(
    slave_path: &Path,
    data_port: u16,
    link: &Path,
    extra_args: &[&str],
) -> (Daemon, u16) {
    let control_port = free_port();
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_sermuxd"));
    cmd.args([
        "--port",
        slave_path.to_str().unwrap(),
        "--bind",
        "127.0.0.1",
        "--data-port",
        &data_port.to_string(),
        "--control-port",
        &control_port.to_string(),
        "--pty-link",
        link.to_str().unwrap(),
    ])
    .args(extra_args)
    .stdout(Stdio::null())
    .stderr(Stdio::null());
    (Daemon(cmd.spawn().unwrap()), control_port)
}

/// Connect a TCP client, retrying until the listener is up.
pub fn connect_client(port: u16) -> TcpStream {
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline {
        match TcpStream::connect(("127.0.0.1", port)) {
            Ok(s) => return s,
            Err(_) => std::thread::sleep(Duration::from_millis(20)),
        }
    }
    panic!("timed out connecting");
}

/// Poll STATUS on the control port until the arbiter reports the default
/// owner (the PTY) — the point at which arbitration is fully armed. Without
/// this, a client write in the window between symlink creation and PTY
/// registration would be dropped and the test would be flaky.
pub fn wait_for_default_owner(control_port: u16) {
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline {
        if let Ok(mut s) = TcpStream::connect(("127.0.0.1", control_port)) {
            let _ = s.set_read_timeout(Some(Duration::from_secs(1)));
            let _ = s.write_all(b"STATUS\n");
            let mut resp = Vec::new();
            let mut byte = [0u8; 1];
            // One line.
            while !resp.ends_with(b"\n") {
                match s.read(&mut byte) {
                    Ok(1) => resp.push(byte[0]),
                    _ => break,
                }
            }
            let text = String::from_utf8_lossy(&resp);
            if text.contains("tx-owner: pty") {
                return;
            }
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("timed out waiting for the default TX owner");
}
