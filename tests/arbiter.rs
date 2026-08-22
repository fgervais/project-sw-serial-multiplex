//! End-to-end test of milestone 3: control port (STATUS/CLAIM/RELEASE), the
//! TOKEN handshake on the data port, forced steals, auto-release on owner
//! disconnect, and the opt-in reject notice.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};

mod common;
use common::{
    TIMEOUT, connect_client, fake_serial_port, free_port, recv_bytes, recv_expect_none,
    spawn_daemon, spawn_reader, wait_for, wait_for_default_owner,
};

/// Send one control command, return the one-line response.
fn cmd(ctrl: &mut TcpStream, line: &str) -> String {
    ctrl.write_all(line.as_bytes()).unwrap();
    ctrl.write_all(b"\n").unwrap();
    let mut out = Vec::new();
    let mut byte = [0u8; 1];
    let deadline = std::time::Instant::now() + TIMEOUT;
    while !out.ends_with(b"\n") {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out reading control response to {line:?}"
        );
        match ctrl.read(&mut byte) {
            Ok(1) => out.push(byte[0]),
            Ok(_) => panic!("control connection closed"),
            Err(e) => panic!("control read error: {e}"),
        }
    }
    String::from_utf8(out).unwrap()
}

/// Extract the hex token from an `OK token: <hex>` response.
fn token_of(response: &str) -> String {
    response
        .trim()
        .strip_prefix("OK token: ")
        .unwrap_or_else(|| panic!("not a token response: {response:?}"))
        .to_string()
}

struct Fixture {
    _daemon: common::Daemon,
    link: PathBuf,
    data_port: u16,
    control_port: u16,
    serial_master: std::fs::File,
}

/// Start a daemon (extra args e.g. `["--reject-notice"]`), wait for the PTY
/// symlink, and return the pieces tests poke at.
fn start(extra: &[&str]) -> Fixture {
    let (slave_path, serial_master) = fake_serial_port();
    let data_port = free_port();
    let link = std::env::temp_dir().join(format!(
        "sermuxd-test-arb-{}-{}",
        std::process::id(),
        data_port
    ));
    let _ = std::fs::remove_file(&link);
    let (daemon, control_port) = spawn_daemon(&slave_path, data_port, &link, extra);
    wait_for(|| link.symlink_metadata().is_ok(), "pty symlink");
    wait_for_default_owner(control_port);
    Fixture {
        _daemon: daemon,
        link,
        data_port,
        control_port,
        serial_master,
    }
}

/// Open the daemon's minicom PTY, like `minicom -D`.
fn open_mux(link: &Path) -> std::fs::File {
    let fd = nix::fcntl::open(
        link,
        nix::fcntl::OFlag::O_RDWR | nix::fcntl::OFlag::O_NOCTTY,
        nix::sys::stat::Mode::empty(),
    )
    .unwrap();
    fd.into()
}

#[test]
fn claim_token_bind_and_release_restores_default() {
    let fx = start(&[]);
    let serial_rx = spawn_reader(fx.serial_master);
    let mut tcp = connect_client(fx.data_port);
    tcp.set_read_timeout(Some(TIMEOUT)).unwrap();
    let mut mux = open_mux(&fx.link);

    // The PTY is the default owner: its writes flow, TCP writes are dropped.
    tcp.write_all(b"tcp tries").unwrap();
    recv_expect_none(&serial_rx, "serial rx from unowned tcp");
    mux.write_all(b"pty default").unwrap();
    assert_eq!(
        recv_bytes(&serial_rx, b"pty default".len(), "serial port"),
        b"pty default"
    );

    // CLAIM hands out a token; a plain CLAIM never fails while the owner is
    // only the *default* (stealing from the PTY needs no --force).
    let mut ctrl = connect_client(fx.control_port);
    ctrl.set_read_timeout(Some(TIMEOUT)).unwrap();
    let token = token_of(&cmd(&mut ctrl, "CLAIM"));

    // The data client presents the token as its first bytes; from then on it
    // owns TX.
    tcp.write_all(format!("TOKEN {token}\n").as_bytes())
        .unwrap();
    tcp.write_all(b"now i own").unwrap();
    assert_eq!(
        recv_bytes(&serial_rx, b"now i own".len(), "serial port"),
        b"now i own"
    );

    // While TCP owns TX, the PTY's writes are dropped.
    mux.write_all(b"pty blocked").unwrap();
    recv_expect_none(&serial_rx, "serial rx from displaced pty");

    // STATUS reports the owner list.
    let status = cmd(&mut ctrl, "STATUS");
    assert!(status.starts_with("OK clients:"), "bad STATUS: {status:?}");
    assert!(
        status.contains("tcp:") && status.contains("pty"),
        "{status:?}"
    );

    // RELEASE with the token hands TX back to the default owner (PTY).
    let released = cmd(&mut ctrl, &format!("RELEASE {token}"));
    assert_eq!(released, "OK released\n");
    mux.write_all(b"pty again").unwrap();
    assert_eq!(
        recv_bytes(&serial_rx, b"pty again".len(), "serial port"),
        b"pty again"
    );
}

#[test]
fn force_steal_and_auto_release_on_disconnect() {
    let fx = start(&[]);
    let serial_rx = spawn_reader(fx.serial_master);
    let mut ctrl = connect_client(fx.control_port);
    ctrl.set_read_timeout(Some(TIMEOUT)).unwrap();

    let mut tcp1 = connect_client(fx.data_port);
    tcp1.set_read_timeout(Some(TIMEOUT)).unwrap();

    // tcp1 claims and owns.
    let token1 = token_of(&cmd(&mut ctrl, "CLAIM"));
    tcp1.write_all(format!("TOKEN {token1}\n").as_bytes())
        .unwrap();
    tcp1.write_all(b"one").unwrap();
    assert_eq!(recv_bytes(&serial_rx, 3, "serial port"), b"one");

    // A plain CLAIM by someone else must now fail; --force steals.
    let err = cmd(&mut ctrl, "CLAIM");
    assert!(err.starts_with("ERR tx held by"), "unexpected: {err:?}");
    let token2 = token_of(&cmd(&mut ctrl, "CLAIM --force"));

    let mut tcp2 = connect_client(fx.data_port);
    tcp2.set_read_timeout(Some(TIMEOUT)).unwrap();
    tcp2.write_all(format!("TOKEN {token2}\n").as_bytes())
        .unwrap();
    tcp2.write_all(b"two").unwrap();
    assert_eq!(recv_bytes(&serial_rx, 3, "serial port"), b"two");

    // The displaced owner's writes go nowhere now.
    tcp1.write_all(b"one-blocked").unwrap();
    recv_expect_none(&serial_rx, "serial rx from stolen owner");

    // The owner disconnects: auto-release falls back to the default (PTY).
    drop(tcp2);
    wait_for(
        || cmd(&mut ctrl, "STATUS").contains("tx-owner: pty"),
        "auto-release after owner disconnect",
    );

    let mut mux = open_mux(&fx.link);
    mux.write_all(b"pty owns again").unwrap();
    assert_eq!(
        recv_bytes(&serial_rx, b"pty owns again".len(), "serial port"),
        b"pty owns again"
    );
}

#[test]
fn reject_notice_is_injected_when_enabled() {
    let fx = start(&["--reject-notice"]);
    let mut tcp = connect_client(fx.data_port);
    tcp.set_read_timeout(Some(TIMEOUT)).unwrap();

    // The client holds no token: its write is dropped and a notice arrives.
    tcp.write_all(b"nope").unwrap();
    let mut got = Vec::new();
    let deadline = std::time::Instant::now() + TIMEOUT;
    let mut buf = [0u8; 256];
    while std::time::Instant::now() < deadline {
        match tcp.read(&mut buf) {
            Ok(n) if n > 0 => {
                got.extend_from_slice(&buf[..n]);
                let text = String::from_utf8_lossy(&got);
                if text.contains("TX denied") {
                    return;
                }
            }
            Ok(_) => std::thread::sleep(std::time::Duration::from_millis(20)),
            Err(e) => panic!("read error: {e}"),
        }
    }
    panic!("no reject notice in: {:?}", String::from_utf8_lossy(&got));
}
