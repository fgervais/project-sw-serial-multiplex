# sermuxd

*Agent-driven project — built and maintained with coding agents.*

A serial port multiplexer daemon. It is the sole owner of a host serial port;
everything else connects as a TCP client and receives all serial output.
Unlike a `socat` 1:1 forward, N clients can be connected at once.

Currently at milestone 2: every byte read from the port is broadcast verbatim
to all connected clients — raw-TCP clients on the data port and a local PTY
client for minicom — and bytes from any client are written to the port (no TX
arbitration yet — like socat, everyone may write). See `PLAN.md` for the full
roadmap (TX-lock arbitration with a control port, replay buffer).

## Prerequisites

- A Rust toolchain (e.g. via [rustup](https://rustup.rs/)).
- Read/write permission on the serial device (e.g. membership in the
  `dialout` group).

## Usage

```bash
cargo build --release
./target/release/sermuxd --port /dev/ttyUSB0 --baud 115200
```

Connect clients to the data port (default `:8000`). Raw, unframed bytes both
ways — no telnet IAC, no CRLF mangling — so existing tooling keeps working:

```bash
nc <host> 8000
# or with pyserial
python3 -c "import serial; s = serial.serial_for_url('socket://<host>:8000')"
```

For local console access, point minicom at the daemon's PTY symlink (default
`/tmp/ttyMUX0`, configurable with `--pty-link`):

```bash
minicom -D /tmp/ttyMUX0
```

A rolling replay buffer (default 256 KiB, tune with `--replay-kb`, 0
disables) keeps recent serial output; every newly connected client receives
a copy of it before joining the live stream, so a late joiner still sees
the boot log.

Run with `--help` for all options (`--port`, `--baud`, `--data-port`,
`--bind`, `--queue-chunks`, `--pty-link`, `--replay-kb`). Logs follow the
`RUST_LOG` env filter, default `info`.
