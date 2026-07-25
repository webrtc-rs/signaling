//! Transport-independent SFU control-plane messages.
//!
//! The gRPC adapter maps these values to `signaling.v2.proto`. Keeping the
//! domain model here lets the signaling authority remain Sans-I/O.

use std::time::Instant;

pub type ConnectionId = u64;
pub type RequestId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Capacity {
    pub max_rooms: u64,
    pub max_clients: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Ready,
    Draining,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Health {
    pub state: State,
    pub capacity: Capacity,
    pub current_rooms: u64,
    pub current_clients: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinMember {
    pub room_id: u64,
    pub client_id: u64,
    pub lifecycle_id: u64,
    pub assignment_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberProjection {
    pub client_id: u64,
    pub lifecycle_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncRoom {
    pub room_id: u64,
    pub assignment_epoch: u64,
    pub members: Vec<MemberProjection>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoomSynced {
    pub room_id: u64,
    pub assignment_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaveMember {
    pub room_id: u64,
    pub client_id: u64,
    pub lifecycle_id: u64,
    pub assignment_epoch: u64,
    pub reason: LeaveReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaveReason {
    User,
    Disconnected,
    RoomClosed,
    WorkerFailure,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Signal {
    pub room_id: u64,
    pub client_id: u64,
    pub lifecycle_id: u64,
    pub assignment_epoch: u64,
    pub message_json: String,
    pub sdp_request_id: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandKind {
    SyncRoom(SyncRoom),
    Join(JoinMember),
    Leave(LeaveMember),
    Signal(Signal),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Command {
    pub request_id: RequestId,
    pub command: CommandKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandOk {
    Acknowledged,
    RoomSynced(RoomSynced),
    MemberJoined(JoinMember),
    MemberLeft(LeaveMember),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    pub reason: String,
    pub retryable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandResult {
    pub request_id: RequestId,
    pub result: Result<CommandOk, Error>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventKind {
    Signal(Signal),
    Health(Health),
    Failure {
        error: Error,
        room_id: Option<u64>,
        client_id: Option<u64>,
        lifecycle_id: Option<u64>,
        sdp_request_id: Option<u64>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    pub request_id: RequestId,
    pub event: EventKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Input {
    Register {
        connection_id: ConnectionId,
        instance_id: String,
        request_id: RequestId,
        capacity: Capacity,
    },
    CommandResult {
        connection_id: ConnectionId,
        instance_id: String,
        result: CommandResult,
        now: Instant,
    },
    Event {
        connection_id: ConnectionId,
        instance_id: String,
        event: Event,
    },
    Disconnected {
        connection_id: ConnectionId,
        instance_id: String,
        now: Instant,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Output {
    Registered {
        connection_id: ConnectionId,
        request_id: RequestId,
        health_interval_ms: u64,
        resumed: bool,
    },
    RegistrationError {
        connection_id: ConnectionId,
        request_id: RequestId,
        error: Error,
    },
    Command {
        connection_id: ConnectionId,
        command: Command,
    },
    EventAck {
        connection_id: ConnectionId,
        request_id: RequestId,
    },
    Close {
        connection_id: ConnectionId,
    },
}
