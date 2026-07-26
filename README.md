<h1 align="center">
 <a href="https://webrtc.rs"><img src="https://raw.githubusercontent.com/webrtc-rs/webrtc-rs.github.io/master/res/apprtc.png" alt="WebRTC.rs"></a>
 <br>
</h1>
<p align="center">
 <a href="https://github.com/webrtc-rs/signaling/actions">
  <img src="https://github.com/webrtc-rs/signaling/workflows/cargo/badge.svg">
 </a>
 <a href="https://codecov.io/gh/webrtc-rs/signaling">
  <img src="https://codecov.io/gh/webrtc-rs/signaling/branch/master/graph/badge.svg">
 </a>
 <a href="https://deps.rs/repo/github/webrtc-rs/signaling">
  <img src="https://deps.rs/repo/github/webrtc-rs/signaling/status.svg">
 </a>
 <a href="https://crates.io/crates/signaling">
  <img src="https://img.shields.io/crates/v/signaling.svg">
 </a>
 <a href="https://docs.rs/signaling">
  <img src="https://docs.rs/signaling/badge.svg">
 </a>
 <a href="https://doc.rust-lang.org/1.6.0/complement-project-faq.html#why-dual-mitasl2-license">
  <img src="https://img.shields.io/badge/license-MIT%2FApache--2.0-blue" alt="License: MIT/Apache 2.0">
 </a>
 <a href="https://discord.gg/4Ju8UHdXMs">
  <img src="https://img.shields.io/discord/800204819540869120?logo=discord" alt="Discord">
 </a>
 <a href="https://twitter.com/WebRTCrs">
  <img src="https://img.shields.io/twitter/url/https/twitter.com/webrtcrs.svg?style=social&label=%40WebRTCrs" alt="Twitter">
 </a>
</p>
<p align="center">
 <strong>Sans-IO Signaling in Rust</strong>
</p>

`signaling` is the authoritative room state machine behind [AppRTC](https://github.com/webrtc-rs/apprtc): who is in
which room, who is the initiator, which SFU worker a room is assigned to, and where each browser frame should go. It
implements both the AppRTC-compatible V1 two-party protocol and the V2 protocol that starts as P2P, upgrades to SFU
media when a third participant joins, and downgrades back to P2P when the room shrinks again.

It is a pure Sans-I/O crate. It owns no socket, thread, clock, or entropy source: the caller supplies every input,
including the current `Instant`, and drains every output. That makes the whole protocol — capacity rules, transition
barriers, epochs, reconnect grace, worker failover — deterministic and testable in memory, and leaves transport choices
to the runtime that drives it.

## Modules

| Module            | Role                                                                                                                                         |
|-------------------|----------------------------------------------------------------------------------------------------------------------------------------------|
| `collider`        | The entry point. Owns both room tables, browser sessions, and the authority API; implements `sansio::Protocol`.                               |
| `room_table`, `room`, `client` | The V1 model: opaque string IDs, at most two clients, initiator election, queued messages, register timeout, reconnect grace.   |
| `v2`              | The V2 model: UUID room ids with `u64` client ids, the room-token codec, admission tokens, signal epochs, room modes, the P2P→SFU join barrier, the SFU→P2P dwell, and the worker registry. |
| `sfu`             | The worker-session model: typed commands (`SyncRoom`, `JoinMember`, `LeaveMember`, `SfuSignal`), their results, worker events, and lifecycle IDs. |
| `messages`        | Browser wire frames, serialized as JSON by the driver.                                                                                        |

[`signaling-proto`](signaling-proto) is a workspace member holding the generated Protobuf/tonic contract that the web
server, this crate's driver, and SFU workers share.

## Driving it

`Collider` implements `sansio::Protocol<BrowserInput, AuthorityCommand, Infallible>`:

| Plane                | Type                                                          |
|----------------------|---------------------------------------------------------------|
| Read in              | `BrowserInput::{Connected, Text, Disconnected}`               |
| Write in             | `AuthorityCommand` — `Admit`, `Remove`, `Occupancy`, `Inject`, `AdmitV2`, `RemoveV2`, `OccupancyV2`, `Status` (`Inject` is V1-only: V2 has no `/message` endpoint) |
| Write out (`poll_write`) | `BrowserOutput::{Text, Close, Sfu}`                       |
| Event out (`poll_event`) | `AuthorityResponse` correlated by `request_id`            |
| Time                 | `Instant`, via `poll_timeout` / `handle_timeout`              |

```rust
use signaling::collider::{BrowserInput, BrowserOutput, Collider};
use sansio::Protocol;
use std::time::{Duration, Instant};

// One Duration configures both the V1 register timeout and the V2 reconnect grace.
let mut collider = Collider::new(Duration::from_secs(10));
collider.set_downgrade_dwell(Duration::from_secs(2)); // SFU→P2P dwell; this is the default

// A browser frame arrives on the driver's WebSocket.
collider.handle_read(BrowserInput::Text {
    connection_id: 1,
    text: r#"{"cmd":"register","roomid":"grYp2g1QjrKVXUZLph46kA","clientid":"101","ver":2,"token":"…"}"#
        .into(),
    now: Instant::now(),
})?;

// Drain whatever that produced: browser frames to write, and SFU worker commands to forward.
while let Some(output) = collider.poll_write() {
    match output {
        BrowserOutput::Text { connection_id, text } => { /* send on that socket */ }
        BrowserOutput::Close { connection_id } => { /* close that socket */ }
        BrowserOutput::Sfu(output) => { /* forward on the worker's gRPC stream */ }
    }
}

// Results of AuthorityCommands come back here, correlated by request_id.
while let Some(response) = collider.poll_event() { /* answer the HTTP caller */ }

// Sleep until this deadline, then hand the time back.
if let Some(deadline) = collider.poll_timeout() { /* … */ }
collider.handle_timeout(Instant::now())?;
```

Worker traffic flows the other way through `handle_sfu_input`, which accepts `sfu::Input::{Register, CommandResult,
Event, Disconnected}`. Nothing in the crate ever calls a worker or a browser directly.

## What the state machine guarantees

- **V1 stays wire-compatible.** Opaque string IDs, a hard cap of two clients with `FULL` on the third join, initiator
  election, queued messages replayed at registration, and silent registration.
- **V2 is token-bound and epoch-gated.** Registration requires an admission token bound to `(room_id, client_id)`, and
  every `send` carries the room's current signal epoch. Frames from a retired epoch are dropped rather than misrouted.
- **A room id has exactly one spelling.** V2 rooms are UUIDv8 values minted by `new_room_id`, carried in links and
  browser JSON as 22 base64url characters. `parse_room_token` accepts only a canonical encoding — right length, zero
  trailing bits, version 8, RFC 9562 variant — because rooms are keyed by the value received, so a second spelling
  would become a second room.
- **Mode transitions are barriers, not flags.** A third join moves the room to `Upgrading`, selects the least-loaded
  ready worker by `(assigned_clients, assigned_rooms, instance_id)`, and commits `SFU` only after every `MemberJoined`
  result arrives. A room that sits at no more than two members for the downgrade dwell commits `P2P` again, elects the
  lowest client id as the direct offerer, and releases the worker.
- **Departures are explicit.** A P2P member gets reconnect grace; an SFU member's disconnect issues an immediate worker
  leave. Either way the survivor of a two-to-one transition is promoted exactly once, on the removal itself.
- **Worker loss is not silently papered over.** A transient stream drop puts the instance in grace and replays
  unacknowledged commands after a `SyncRoom`; grace expiry fails the assigned rooms instead of migrating live transports.

## Testing

```bash
cargo test --workspace --lib
```

The suite is in-memory and clock-free: tests drive `Collider` and the V2 table directly and step time by passing
`Instant`s, so upgrade ordering, stale-epoch drops, dwell expiry, worker reconnect, and grace expiry are all exercised
without sockets.

## Using it

The reference driver is [`apprtc`](https://github.com/webrtc-rs/apprtc), which supplies the browser WebSocket listener,
the private gRPC service for the web server and SFU workers, and the single-owner event loop that serializes every
operation against one `Collider`.
