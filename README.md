# sermuxd

*Agent-driven project — built and maintained with coding agents.*

A serial port multiplexer daemon. It is the sole owner of a host serial port;
everything else connects as a TCP client and receives all serial output.
Unlike a `socat` 1:1 forward, N clients can be connected at once, and a
TX-lock arbiter decides who may write.

Currently at milestone 3: every byte read from the port is broadcast verbatim
to all connected clients — raw-TCP clients on the data port and a local PTY
client for minicom — while writes to the port are gated by a control port
(`CLAIM`/`RELEASE`/`STATUS`) handing out tokens. The PTY client is the
default TX owner. See `PLAN.md` for the full roadmap.

## Prerequisites

- A Rust toolchain (e.g. via [rustup](https://rustup.rs/)).
- Read/write permission on the serial device (e.g. membership in the
  `dialout` group).

## Usage

```bash
cargo build --release
./target/release/sermuxd --port /dev/ttyUSB0 --baud 115200
```

Clients connect to the data port (default `:8000`) — raw, unframed bytes both
ways, no telnet IAC, no CRLF mangling. For a local console, point minicom at
the PTY symlink (default `/tmp/ttyMUX0`, configurable with `--pty-link`):

```bash
minicom -D /tmp/ttyMUX0
```

## TX arbitration

Only the client holding the TX lock may write to the serial port; everyone
else's TX bytes are dropped (RX still flows to all). The PTY client (minicom)
is the default owner.

The control port (default `:8001`) speaks a line-based protocol (`nc` or any
TCP client):

```
STATUS
> OK clients: 2 (pty, tcp:10.0.0.5:52344)  tx-owner: pty

CLAIM
> OK token: 4f3a2b1c9d0e8f7a

RELEASE <token>
> OK released
```

Commands: `STATUS`, `CLAIM`, `CLAIM --force` (steal from a bound owner),
`RELEASE [token]`, `HELP`. Stealing from the *default* PTY owner needs no
`--force`.

A `CLAIM` returns a **token**, which the claiming tool must present on the
**data port** to take ownership: send `TOKEN <hex>\n` as the first bytes of a
chunk on your data connection — it is consumed as a handshake, never reaches
the serial wire, and makes that connection the TX owner. Ownership is bound
to that data connection, so **a crashed client can never wedge the port**:
disconnect auto-releases back to the default owner. Unclaimed pending tokens
are cleaned up when the control connection that issued them closes.

Caveat: a client chunk starting exactly with `TOKEN ` (followed by hex and
`\n`) is interpreted as the handshake, not raw data. Tools that claim must
send it as a standalone write; purely passive clients are unaffected.

With `--reject-notice`, a one-line ASCII notice is injected into a client's
own stream when its TX bytes are dropped (handy for humans, off for parsing).

A rolling replay buffer (default 256 KiB, tune with `--replay-kb`, 0
disables) keeps recent serial output; every newly connected client receives
a copy before joining the live stream, so a late joiner still sees the boot
log.

Run with `--help` for all options (`--port`, `--baud`, `--data-port`,
`--control-port`, `--bind`, `--queue-chunks`, `--pty-link`, `--replay-kb`,
`--reject-notice`). Logs follow the `RUST_LOG` env filter, default `info`.
