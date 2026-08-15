# Serial Multiplexer — Project Plan

## Problem

Today, `socat` forwards the host's serial port (`/dev/ttyUSB0`) to TCP, and the
agent connects over raw TCP (see the `serial-console` skill). This works, but
socat is a strict 1:1 pipe: while the agent holds the port, the console is no
longer usable from the host (e.g. with minicom), and there is no protection
against two writers colliding.

## Goal

A single daemon, running on the host, that is the **sole owner of the serial
port**. Everything else — minicom, the agent, other tools — connects as a
client:

- **N clients** connected concurrently, all receiving the serial output.
- **Exactly one client may transmit** at a time (TX lock with arbitration).
- **Local minicom stays fully transparent** via a PTY client.
- **Explicit TX arbitration** with minicom (PTY) as the default owner.
- Single static binary, built in **Rust**.

## Architecture

```
                 ┌─────────────────────────────┐
 /dev/ttyUSB0 ◄──┤  serial task (owns port)    │
 115200 8N1      │    │           ▲            │
                 │    ▼ rx        │ tx (locked)│
                 │  broadcast ──► arbiter task │
                 │    │           ▲            │
                 └────┼───────────┼────────────┘
                      │           │
        ┌─────────────┼───────────┼──────────┐
        ▼             ▼           ▼          ▼
   PTY client    TCP :8000   TCP :8000   control :8001
   (minicom)     (agent)     (other)     (line commands)
```

### Components

1. **Serial task** — owns `/dev/ttyUSB0` exclusively. Every chunk read is
   published verbatim to a `tokio::sync::broadcast` channel; all clients
   subscribe, so everyone receives everything. Writes to the port are
   serialized through a single channel guarded by the arbiter.

2. **PTY client** — `openpty()` pair; the slave side is symlinked to a stable
   path (e.g. `/tmp/ttyMUX0`, configurable). Point minicom at it:
   `minicom -D /tmp/ttyMUX0`. Baud-rate ioctls on a PTY are harmless no-ops,
   so minicom's settings "just work". Internally the PTY is an ordinary
   client; per policy it **starts as TX owner**.

3. **TCP data listener** (default `:8000`) — raw, unframed bytes both ways,
   exactly like the current socat setup. No telnet IAC, no CRLF mangling, no
   escape sequences, no line buffering. Binary protocols (XMODEM uploads,
   ANSI escapes in boot logs) pass through untouched. Existing
   pyserial/`socket://` client code keeps working unchanged. Incoming bytes
   from a client that does **not** hold the TX lock are dropped (optional
   one-line ASCII notice injected into that client's stream, opt-in).

4. **TCP control listener** (default `:8001`) — separate, line-based text
   protocol, never touches the serial stream. Keeps the data path clean
   (no in-band escapes; see "Rejected alternatives" below).

   Example session (`nc localhost 8001`):
   ```
   STATUS
   > clients: 3 (pty:minicom, tcp:10.0.0.5, tcp:10.0.0.6)  tx-owner: pty:minicom
   CLAIM
   > OK, you hold TX
   RELEASE
   > OK, released
   ```

   Commands (initial set):
   - `STATUS` — list clients and current TX owner.
   - `CLAIM` — acquire TX lock if free.
   - `CLAIM --force` — steal the lock from the current owner.
   - `CLAIM --wait` — (stretch) block until the lock is free.
   - `RELEASE` — give up the lock.

5. **Arbiter** — TX-lock state machine:
   - Default owner at startup: the PTY client (minicom).
   - Lock granted to exactly one client at a time.
   - **Auto-release on owner disconnect**, so a crashed client can never
     wedge the port.
   - Ownership identity: the claiming control connection returns a token;
     the data client presents the token on connect to be recognized as TX
     owner (NAT-proof; decided for milestone 3).

## Buffering

Buffering never alters the byte stream; tuned so a slow client cannot hurt
others.

- **Fan-out:** per-client bounded queue (`tokio::sync::broadcast`, ~64 KB).
  A lagging client drops data **for that client only** (loudly — the gap is
  signaled); block-everyone-behind-the-slowest-client is explicitly rejected.
- **Serial RX:** read in 4–8 KB chunks, fanned out immediately.
  Sub-millisecond + network latency; kernel serial buffers absorb bursts.
- **TX:** minimal channel from lock holder to serial task. No line buffering
  — a typed character is sent immediately, like a real port.
- **Replay buffer (stretch, off by default):** ring buffer of the last
  ~256 KB of serial output, sent to newly connected clients so a late joiner
  sees recent history (e.g. boot log) before joining the live stream.

## Build & packaging

- Language: **Rust**.
- Crates: `serialport`, `tokio`, `tokio-util`, `clap`, `nix` (PTY). All pure
  Rust — no C dependencies.
- Default build already produces a single self-contained binary (dynamic
  glibc). For a fully static binary:
  ```bash
  rustup target add x86_64-unknown-linux-musl
  cargo build --release --target x86_64-unknown-linux-musl
  ```
- `Cargo.toml` release profile: `lto = true`, `strip = true`
  (single stripped executable, expected ~1–3 MB).

## CLI sketch

```
sermuxd --port /dev/ttyUSB0 --baud 115200 \
        --data-port 8000 --control-port 8001 \
        --pty-link /tmp/ttyMUX0
```

## Milestones

1. **Socat replacement:** serial↔broadcast hub + TCP data clients, no
   arbitration (everyone can write, like socat today). Already useful.
2. **PTY client:** local minicom via `/tmp/ttyMUX0`.
3. **Arbiter + control port:** TX lock, `CLAIM`/`RELEASE`/`STATUS`, token
   identity, auto-release on disconnect, reject notice (opt-in).
4. **Polish:** replay buffer, wire logging (timestamped capture of
   everything on the wire), systemd unit, `CLAIM --wait`.

## Rejected / deferred alternatives

- **In-band escape sequences (minicom-style `Ctrl-A`) for control** —
  deferred, documented as a possible later option (off by default,
  configurable prefix). Reasons for rejecting as the primary mechanism:
  - Breaks byte-for-byte TX transparency; any prefix has a collision set.
    Concrete collision: XMODEM uses SOH (`0x01`) as its packet header —
    `Ctrl-A` escapes would corrupt firmware uploads via `sx`/lrzsz/u-boot.
  - minicom consumes `Ctrl-A` locally; the escape would need awkward
    doubling to pass through — it fights minicom instead of feeling like it.
  - Command replies injected into RX pollute the stream for parsing/logging.
  - Requires a per-connection state machine (prefix split across TCP reads,
    timeouts, escaping) — the fiddliest code in the project, to save the
    client one TCP connection.
- **HTTP control endpoint** instead of raw line-based TCP on :8001 —
  possible later; line-based TCP is simpler and dependency-free.
