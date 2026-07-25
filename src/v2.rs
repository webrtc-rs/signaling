//! SFU-capable V2 room and worker state.
//!
//! Identifiers are numeric, browser registrations are admission-token bound,
//! and P2P-to-SFU upgrades commit only after every `JoinMember` command has
//! succeeded.

use crate::sfu;
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

const MAX_P2P_MEMBERS: usize = 2;
const MAX_QUEUED_MESSAGES: usize = 1024;
const HEALTH_INTERVAL_MS: u64 = 30_000;
const EVENT_DEDUP_CAPACITY: usize = 4096;
/// How long an SFU room must sit at ≤2 members before it downgrades back to
/// direct P2P. The dwell absorbs brief membership churn (a third participant
/// leaving and immediately rejoining) so the room does not flap between modes.
const DOWNGRADE_DWELL: Duration = Duration::from_secs(2);

pub type RoomId = u64;
pub type ClientId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoomMode {
    P2p,
    Upgrading,
    Sfu,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Admission {
    pub mode: RoomMode,
    pub signal_epoch: u64,
    pub admission_token: String,
    pub is_initiator: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdmissionResult {
    Complete(Admission),
    Pending,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemovalResult {
    Complete(Option<Promotion>),
    Pending,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Registration {
    pub mode: RoomMode,
    pub signal_epoch: u64,
    pub is_initiator: Option<bool>,
    pub queued_messages: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery {
    pub room_id: RoomId,
    pub client_id: ClientId,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Promotion {
    pub room_id: RoomId,
    pub client_id: ClientId,
    pub signal_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SendResult {
    P2p(Option<Delivery>),
    Sfu,
    Dropped,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    Sfu(sfu::Output),
    AdmissionCompleted {
        authority_request_id: u64,
        result: Result<Admission, String>,
    },
    RemovalCompleted {
        authority_request_id: u64,
        room_id: RoomId,
        client_id: ClientId,
        result: Result<(), String>,
    },
    Upgraded {
        room_id: RoomId,
        signal_epoch: u64,
        existing_clients: Vec<ClientId>,
    },
    Downgraded {
        room_id: RoomId,
        signal_epoch: u64,
        /// The member elected to offer the direct P2P connection; every other
        /// member becomes the answerer.
        initiator_client_id: ClientId,
        clients: Vec<ClientId>,
    },
    Deliver(Delivery),
    RoomFailed {
        room_id: RoomId,
        clients: Vec<ClientId>,
        reason: String,
    },
}

struct Member {
    token: String,
    is_initiator: bool,
    lifecycle_id: u64,
    registered: bool,
    reconnect_deadline: Option<Instant>,
    queued_messages: VecDeque<String>,
}

struct Upgrade {
    authority_request_id: u64,
    joining_client_id: ClientId,
    ordered_clients: Vec<ClientId>,
    joined_clients: Vec<ClientId>,
    next_client: usize,
}

struct PendingJoin {
    authority_request_id: u64,
    client_id: ClientId,
}

struct PendingLeave {
    authority_request_id: Option<u64>,
    client_id: ClientId,
}

struct Room {
    members: HashMap<ClientId, Member>,
    mode: RoomMode,
    signal_epoch: u64,
    assignment_epoch: u64,
    assigned_instance: Option<String>,
    upgrade: Option<Upgrade>,
    pending_join: Option<PendingJoin>,
    pending_leave: Option<PendingLeave>,
    /// When an SFU room shrinks to at most two members it becomes eligible for a
    /// downgrade back to direct P2P. This is the deadline after which the dwell
    /// expires and `commit_downgrade` runs; cleared whenever the room stops being
    /// a stable ≤2-member SFU room (a third joins, or a transition starts).
    downgrade_deadline: Option<Instant>,
}

struct Worker {
    connection_id: Option<sfu::ConnectionId>,
    capacity: sfu::Capacity,
    health: Option<sfu::Health>,
    assigned_rooms: HashSet<RoomId>,
    assigned_clients: usize,
    seen_events: HashSet<u64>,
    seen_event_order: VecDeque<u64>,
    disconnect_deadline: Option<Instant>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PendingCommand {
    SyncRoom {
        instance_id: String,
        room_id: RoomId,
        assignment_epoch: u64,
        replay: Vec<sfu::Command>,
    },
    UpgradeJoin {
        instance_id: String,
        room_id: RoomId,
        client_id: ClientId,
        lifecycle_id: u64,
        assignment_epoch: u64,
    },
    SfuJoin {
        instance_id: String,
        room_id: RoomId,
        client_id: ClientId,
        lifecycle_id: u64,
        assignment_epoch: u64,
    },
    SfuLeave {
        instance_id: String,
        room_id: RoomId,
        client_id: ClientId,
        lifecycle_id: u64,
        assignment_epoch: u64,
    },
    Signal,
    Cleanup,
}

pub struct RoomTable {
    rooms: HashMap<RoomId, Room>,
    workers: HashMap<String, Worker>,
    lifecycle_ids: HashMap<(RoomId, ClientId), u64>,
    pending_commands: HashMap<u64, PendingCommand>,
    unacknowledged_commands: HashMap<u64, sfu::Command>,
    command_workers: HashMap<u64, String>,
    next_sfu_request_id: u64,
    reconnect_grace: Duration,
    downgrade_dwell: Duration,
    actions: VecDeque<Action>,
}

impl RoomTable {
    pub fn new(reconnect_grace: Duration) -> Self {
        Self {
            rooms: HashMap::new(),
            workers: HashMap::new(),
            lifecycle_ids: HashMap::new(),
            pending_commands: HashMap::new(),
            unacknowledged_commands: HashMap::new(),
            command_workers: HashMap::new(),
            next_sfu_request_id: 1,
            reconnect_grace,
            downgrade_dwell: DOWNGRADE_DWELL,
            actions: VecDeque::new(),
        }
    }

    /// Override the SFU→P2P downgrade dwell (default [`DOWNGRADE_DWELL`]). Lets a deployment tune
    /// how long a room lingers at two SFU members before returning to direct P2P.
    pub fn set_downgrade_dwell(&mut self, dwell: Duration) {
        self.downgrade_dwell = dwell;
    }

    pub fn admit(
        &mut self,
        authority_request_id: u64,
        now: Instant,
        room_id: RoomId,
        client_id: ClientId,
        admission_token: String,
    ) -> Result<AdmissionResult, String> {
        if self
            .rooms
            .get(&room_id)
            .is_some_and(|room| room.members.contains_key(&client_id))
        {
            return Err("DUPLICATE_CLIENT".into());
        }

        if !self.rooms.contains_key(&room_id) {
            let lifecycle_id = self.next_lifecycle(room_id, client_id);
            let mut members = HashMap::new();
            members.insert(
                client_id,
                new_member(
                    now,
                    self.reconnect_grace,
                    admission_token.clone(),
                    true,
                    lifecycle_id,
                ),
            );
            self.rooms.insert(
                room_id,
                Room {
                    members,
                    mode: RoomMode::P2p,
                    signal_epoch: 0,
                    assignment_epoch: 0,
                    assigned_instance: None,
                    upgrade: None,
                    pending_join: None,
                    pending_leave: None,
                    downgrade_deadline: None,
                },
            );
            return Ok(AdmissionResult::Complete(Admission {
                mode: RoomMode::P2p,
                signal_epoch: 0,
                admission_token,
                is_initiator: Some(true),
            }));
        }

        let mode = self.rooms[&room_id].mode;
        match mode {
            RoomMode::Upgrading => return Err("ROOM_TRANSITION".into()),
            RoomMode::Failed => return Err("WORKER_UNAVAILABLE".into()),
            RoomMode::Sfu => {
                if self.rooms[&room_id].pending_join.is_some()
                    || self.rooms[&room_id].pending_leave.is_some()
                {
                    return Err("ROOM_TRANSITION".into());
                }
                return self.admit_to_sfu(
                    authority_request_id,
                    now,
                    room_id,
                    client_id,
                    admission_token,
                );
            }
            RoomMode::P2p => {}
        }

        if self.rooms[&room_id].members.len() < MAX_P2P_MEMBERS {
            let lifecycle_id = self.next_lifecycle(room_id, client_id);
            let room = self.rooms.get_mut(&room_id).expect("room checked");
            room.members.insert(
                client_id,
                new_member(
                    now,
                    self.reconnect_grace,
                    admission_token.clone(),
                    false,
                    lifecycle_id,
                ),
            );
            return Ok(AdmissionResult::Complete(Admission {
                mode: RoomMode::P2p,
                signal_epoch: room.signal_epoch,
                admission_token,
                is_initiator: Some(false),
            }));
        }

        let instance_id = self.select_worker(3).ok_or("NO_SFU_AVAILABLE")?;
        let lifecycle_id = self.next_lifecycle(room_id, client_id);
        let room = self.rooms.get_mut(&room_id).expect("room checked");
        room.members.insert(
            client_id,
            new_member(
                now,
                self.reconnect_grace,
                admission_token,
                false,
                lifecycle_id,
            ),
        );
        room.mode = RoomMode::Upgrading;
        room.assignment_epoch = room.assignment_epoch.saturating_add(1);
        room.assigned_instance = Some(instance_id.clone());
        let mut ordered_clients = room.members.keys().copied().collect::<Vec<_>>();
        ordered_clients.sort_unstable();
        // The new member is always joined last, matching the documented barrier.
        ordered_clients.retain(|candidate| *candidate != client_id);
        ordered_clients.push(client_id);
        // A worker membership lifecycle begins at the upgrade boundary. Existing
        // P2P members therefore receive fresh lifecycle IDs as well.
        for existing_client in ordered_clients
            .iter()
            .copied()
            .filter(|candidate| *candidate != client_id)
        {
            let lifecycle = self.next_lifecycle(room_id, existing_client);
            self.rooms
                .get_mut(&room_id)
                .and_then(|room| room.members.get_mut(&existing_client))
                .expect("existing upgrade member")
                .lifecycle_id = lifecycle;
        }
        let room = self.rooms.get_mut(&room_id).expect("room checked");
        room.upgrade = Some(Upgrade {
            authority_request_id,
            joining_client_id: client_id,
            ordered_clients,
            joined_clients: Vec::new(),
            next_client: 0,
        });
        let worker = self.workers.get_mut(&instance_id).expect("selected worker");
        worker.assigned_rooms.insert(room_id);
        worker.assigned_clients = worker.assigned_clients.saturating_add(3);
        self.issue_next_upgrade_join(room_id)?;
        Ok(AdmissionResult::Pending)
    }

    pub fn register(
        &mut self,
        room_id: RoomId,
        client_id: ClientId,
        token: &str,
    ) -> Result<Registration, String> {
        let room = self.rooms.get_mut(&room_id).ok_or("UNAUTHORIZED")?;
        if room.mode == RoomMode::Upgrading {
            return Err("ROOM_TRANSITION".into());
        }
        let is_initiator = {
            let member = room.members.get_mut(&client_id).ok_or("UNAUTHORIZED")?;
            if !constant_time_eq(member.token.as_bytes(), token.as_bytes()) {
                return Err("UNAUTHORIZED".into());
            }
            if member.registered {
                return Err("Duplicated registration".into());
            }
            member.registered = true;
            member.reconnect_deadline = None;
            member.is_initiator
        };
        let mut queued_messages = Vec::new();
        if room.mode == RoomMode::P2p {
            for (&other_id, other) in &mut room.members {
                if other_id != client_id {
                    queued_messages.extend(other.queued_messages.drain(..));
                }
            }
        }
        Ok(Registration {
            mode: room.mode,
            signal_epoch: room.signal_epoch,
            is_initiator: (room.mode == RoomMode::P2p).then_some(is_initiator),
            queued_messages,
        })
    }

    fn admit_to_sfu(
        &mut self,
        authority_request_id: u64,
        now: Instant,
        room_id: RoomId,
        client_id: ClientId,
        admission_token: String,
    ) -> Result<AdmissionResult, String> {
        let instance_id = self.rooms[&room_id]
            .assigned_instance
            .clone()
            .ok_or("WORKER_UNAVAILABLE")?;
        let connection_id = self
            .workers
            .get(&instance_id)
            .and_then(|worker| worker.connection_id)
            .ok_or("WORKER_UNAVAILABLE")?;
        let lifecycle_id = self.next_lifecycle(room_id, client_id);
        let room = self.rooms.get_mut(&room_id).expect("room checked");
        room.members.insert(
            client_id,
            new_member(
                now,
                self.reconnect_grace,
                admission_token,
                false,
                lifecycle_id,
            ),
        );
        room.pending_join = Some(PendingJoin {
            authority_request_id,
            client_id,
        });
        // A new member is joining, so the room is no longer a downgrade candidate.
        room.downgrade_deadline = None;
        let assignment_epoch = room.assignment_epoch;
        let request_id = self.next_command_id();
        self.queue_command(
            instance_id.clone(),
            connection_id,
            sfu::Command {
                request_id,
                command: sfu::CommandKind::Join(sfu::JoinMember {
                    room_id,
                    client_id,
                    lifecycle_id,
                    assignment_epoch,
                }),
            },
            PendingCommand::SfuJoin {
                instance_id,
                room_id,
                client_id,
                lifecycle_id,
                assignment_epoch,
            },
        );
        Ok(AdmissionResult::Pending)
    }

    pub fn deregister(&mut self, now: Instant, room_id: RoomId, client_id: ClientId) {
        let is_sfu = {
            let Some(room) = self.rooms.get_mut(&room_id) else {
                return;
            };
            let is_sfu = room.mode == RoomMode::Sfu;
            let Some(member) = room.members.get_mut(&client_id) else {
                return;
            };
            if !member.registered {
                return;
            }
            member.registered = false;
            is_sfu
        };
        // Match the chat.rs example: an SFU member whose WebSocket drops (browser close, refresh,
        // crash) is treated as an *immediate* leave — issue the `Leave` command to the worker now
        // so its forwarded media stops and the other clients' grid tiles prune without waiting out
        // the reconnect grace. Only fall back to the grace deadline when the leave can't be issued
        // yet (worker unavailable or a leave already pending), where `handle_timeout` retries it.
        if is_sfu && self.issue_sfu_leave(room_id, client_id, sfu::LeaveReason::Disconnected) {
            return;
        }
        // P2P and upgrading members keep the reconnect grace so a quick refresh can rejoin without
        // collapsing the call or prematurely promoting the survivor.
        if let Some(member) = self
            .rooms
            .get_mut(&room_id)
            .and_then(|room| room.members.get_mut(&client_id))
        {
            member.reconnect_deadline = Some(now + self.reconnect_grace);
        }
    }

    /// Queue an immediate `Leave` command to the SFU worker owning `room_id`, recording the room's
    /// pending leave. Returns `false` (queuing nothing) when the room/member is gone, a leave is
    /// already in flight, or no worker connection is available; the caller then falls back to a
    /// reconnect deadline that `handle_timeout` retries.
    fn issue_sfu_leave(
        &mut self,
        room_id: RoomId,
        client_id: ClientId,
        reason: sfu::LeaveReason,
    ) -> bool {
        let Some(room) = self.rooms.get(&room_id) else {
            return false;
        };
        if room.mode != RoomMode::Sfu || room.pending_leave.is_some() {
            return false;
        }
        let Some(member) = room.members.get(&client_id) else {
            return false;
        };
        let lifecycle_id = member.lifecycle_id;
        let assignment_epoch = room.assignment_epoch;
        let Some(instance_id) = room.assigned_instance.clone() else {
            return false;
        };
        let Some(connection_id) = self
            .workers
            .get(&instance_id)
            .and_then(|worker| worker.connection_id)
        else {
            return false;
        };
        let room = self.rooms.get_mut(&room_id).expect("room checked");
        room.pending_leave = Some(PendingLeave {
            authority_request_id: None,
            client_id,
        });
        let request_id = self.next_command_id();
        self.queue_command(
            instance_id.clone(),
            connection_id,
            sfu::Command {
                request_id,
                command: sfu::CommandKind::Leave(sfu::LeaveMember {
                    room_id,
                    client_id,
                    lifecycle_id,
                    assignment_epoch,
                    reason,
                }),
            },
            PendingCommand::SfuLeave {
                instance_id,
                room_id,
                client_id,
                lifecycle_id,
                assignment_epoch,
            },
        );
        true
    }

    pub fn send(
        &mut self,
        room_id: RoomId,
        client_id: ClientId,
        signal_epoch: u64,
        message: String,
    ) -> Result<SendResult, String> {
        let room = self.rooms.get_mut(&room_id).ok_or("CLIENT_NOT_FOUND")?;
        if room.signal_epoch != signal_epoch || room.mode == RoomMode::Upgrading {
            return Ok(SendResult::Dropped);
        }
        let member = room.members.get(&client_id).ok_or("CLIENT_NOT_FOUND")?;
        if room.mode == RoomMode::Sfu {
            let instance_id = room.assigned_instance.clone().ok_or("WORKER_UNAVAILABLE")?;
            let connection_id = self
                .workers
                .get(&instance_id)
                .and_then(|worker| worker.connection_id)
                .ok_or("WORKER_UNAVAILABLE")?;
            let signal = sfu::Signal {
                room_id,
                client_id,
                lifecycle_id: member.lifecycle_id,
                assignment_epoch: room.assignment_epoch,
                message_json: message,
                sdp_request_id: None,
            };
            let request_id = self.next_command_id();
            self.queue_command(
                instance_id,
                connection_id,
                sfu::Command {
                    request_id,
                    command: sfu::CommandKind::Signal(signal),
                },
                PendingCommand::Signal,
            );
            return Ok(SendResult::Sfu);
        }

        let peer_id = room.members.keys().find(|&&id| id != client_id).copied();
        let Some(peer_id) = peer_id else {
            let source = room.members.get_mut(&client_id).expect("member checked");
            push_queued(source, message)?;
            return Ok(SendResult::P2p(None));
        };
        if room
            .members
            .get(&peer_id)
            .is_some_and(|peer| peer.registered)
        {
            return Ok(SendResult::P2p(Some(Delivery {
                room_id,
                client_id: peer_id,
                message,
            })));
        }
        let source = room.members.get_mut(&client_id).expect("member checked");
        push_queued(source, message)?;
        Ok(SendResult::P2p(None))
    }

    pub fn handle_sfu_input(&mut self, input: sfu::Input) -> Result<(), String> {
        match input {
            sfu::Input::Register {
                connection_id,
                instance_id,
                request_id,
                capacity,
            } => self.register_worker(connection_id, instance_id, request_id, capacity),
            sfu::Input::CommandResult {
                connection_id,
                instance_id,
                result,
                now,
            } => self.handle_command_result(connection_id, &instance_id, result, now),
            sfu::Input::Event {
                connection_id,
                instance_id,
                event,
            } => self.handle_worker_event(connection_id, &instance_id, event),
            sfu::Input::Disconnected {
                connection_id,
                instance_id,
                now,
            } => {
                if let Some(worker) = self.workers.get_mut(&instance_id)
                    && worker.connection_id == Some(connection_id)
                {
                    worker.connection_id = None;
                    worker.health = None;
                    worker.disconnect_deadline = Some(now + self.reconnect_grace);
                }
                Ok(())
            }
        }
    }

    fn register_worker(
        &mut self,
        connection_id: sfu::ConnectionId,
        instance_id: String,
        request_id: u64,
        capacity: sfu::Capacity,
    ) -> Result<(), String> {
        if instance_id.is_empty()
            || request_id == 0
            || capacity.max_rooms == 0
            || capacity.max_clients == 0
        {
            self.actions
                .push_back(Action::Sfu(sfu::Output::RegistrationError {
                    connection_id,
                    request_id,
                    error: sfu::Error {
                        reason: "INVALID_REQUEST".into(),
                        retryable: false,
                    },
                }));
            return Ok(());
        }
        let resumed = self.workers.contains_key(&instance_id);
        let old_connection = self
            .workers
            .get(&instance_id)
            .and_then(|worker| worker.connection_id);
        let worker = self
            .workers
            .entry(instance_id.clone())
            .or_insert_with(|| Worker {
                connection_id: None,
                capacity,
                health: None,
                assigned_rooms: HashSet::new(),
                assigned_clients: 0,
                seen_events: HashSet::new(),
                seen_event_order: VecDeque::new(),
                disconnect_deadline: None,
            });
        worker.connection_id = Some(connection_id);
        worker.capacity = capacity;
        worker.health = None;
        worker.disconnect_deadline = None;
        if let Some(old_connection) = old_connection.filter(|old| *old != connection_id) {
            self.actions.push_back(Action::Sfu(sfu::Output::Close {
                connection_id: old_connection,
            }));
        }
        self.actions.push_back(Action::Sfu(sfu::Output::Registered {
            connection_id,
            request_id,
            health_interval_ms: HEALTH_INTERVAL_MS,
            resumed,
        }));
        let superseded_syncs = self
            .pending_commands
            .iter()
            .filter_map(|(&request_id, pending)| {
                matches!(
                    pending,
                    PendingCommand::SyncRoom {
                        instance_id: pending_instance,
                        ..
                    } if pending_instance == &instance_id
                )
                .then_some(request_id)
            })
            .collect::<Vec<_>>();
        for request_id in superseded_syncs {
            self.pending_commands.remove(&request_id);
            self.unacknowledged_commands.remove(&request_id);
            self.command_workers.remove(&request_id);
        }
        let mut assigned_rooms = self
            .workers
            .get(&instance_id)
            .map(|worker| worker.assigned_rooms.iter().copied().collect::<Vec<_>>())
            .unwrap_or_default();
        assigned_rooms.sort_unstable();
        for room_id in assigned_rooms {
            let Some(room) = self.rooms.get(&room_id) else {
                continue;
            };
            let assignment_epoch = room.assignment_epoch;
            let members = room
                .members
                .iter()
                .map(|(&client_id, member)| sfu::MemberProjection {
                    client_id,
                    lifecycle_id: member.lifecycle_id,
                })
                .collect();
            let replay = self
                .command_workers
                .iter()
                .filter(|(_, worker)| worker.as_str() == instance_id)
                .filter(|(request_id, _)| {
                    !matches!(
                        self.pending_commands.get(request_id),
                        Some(PendingCommand::SyncRoom { .. })
                    )
                })
                .filter_map(|(request_id, _)| self.unacknowledged_commands.get(request_id))
                .filter(|command| command_room_id(command) == Some(room_id))
                .cloned()
                .collect::<Vec<_>>();
            let request_id = self.next_command_id();
            self.queue_command(
                instance_id.clone(),
                connection_id,
                sfu::Command {
                    request_id,
                    command: sfu::CommandKind::SyncRoom(sfu::SyncRoom {
                        room_id,
                        assignment_epoch,
                        members,
                    }),
                },
                PendingCommand::SyncRoom {
                    instance_id: instance_id.clone(),
                    room_id,
                    assignment_epoch,
                    replay,
                },
            );
        }
        Ok(())
    }

    fn handle_worker_event(
        &mut self,
        connection_id: sfu::ConnectionId,
        instance_id: &str,
        event: sfu::Event,
    ) -> Result<(), String> {
        let worker = self.workers.get_mut(instance_id).ok_or("UNKNOWN_SFU")?;
        if worker.connection_id != Some(connection_id) || event.request_id == 0 {
            return Err("STALE_SFU_SESSION".into());
        }
        if worker.seen_events.contains(&event.request_id) {
            self.actions.push_back(Action::Sfu(sfu::Output::EventAck {
                connection_id,
                request_id: event.request_id,
            }));
            return Ok(());
        }
        worker.seen_events.insert(event.request_id);
        worker.seen_event_order.push_back(event.request_id);
        if worker.seen_event_order.len() > EVENT_DEDUP_CAPACITY
            && let Some(oldest) = worker.seen_event_order.pop_front()
        {
            worker.seen_events.remove(&oldest);
        }
        match event.event {
            sfu::EventKind::Health(health) => {
                log::info!(
                    "SFU health: instance_id={instance_id} connection_id={connection_id} request_id={} state={:?} rooms={} clients={}",
                    event.request_id,
                    health.state,
                    health.current_rooms,
                    health.current_clients
                );
                worker.capacity = health.capacity;
                worker.health = Some(health);
            }
            sfu::EventKind::Signal(signal) => {
                let room = self.rooms.get(&signal.room_id).ok_or("ROOM_NOT_FOUND")?;
                let member = room
                    .members
                    .get(&signal.client_id)
                    .ok_or("CLIENT_NOT_FOUND")?;
                if room.mode != RoomMode::Sfu
                    || room.assigned_instance.as_deref() != Some(instance_id)
                    || room.assignment_epoch != signal.assignment_epoch
                    || member.lifecycle_id != signal.lifecycle_id
                {
                    return Err("STALE_ASSIGNMENT".into());
                }
                // The worker-originated SDP (a publish answer or a server-initiated subscribe
                // re-offer) and its candidates. Log the body to show
                // the worker->signaling leg (before signaling relays it on to the browser).
                log::info!(
                    "SFU event: instance_id={instance_id} operation=signal room_id={} client_id={}\n{}",
                    signal.room_id,
                    signal.client_id,
                    signal.message_json
                );
                self.actions.push_back(Action::Deliver(Delivery {
                    room_id: signal.room_id,
                    client_id: signal.client_id,
                    message: signal.message_json,
                }));
            }
            sfu::EventKind::Failure { error, room_id, .. } => {
                if let Some(room_id) = room_id {
                    self.fail_room(room_id, error.reason);
                }
            }
        }
        self.actions.push_back(Action::Sfu(sfu::Output::EventAck {
            connection_id,
            request_id: event.request_id,
        }));
        Ok(())
    }

    fn handle_command_result(
        &mut self,
        connection_id: sfu::ConnectionId,
        instance_id: &str,
        result: sfu::CommandResult,
        now: Instant,
    ) -> Result<(), String> {
        let worker = self.workers.get(instance_id).ok_or("UNKNOWN_SFU")?;
        if worker.connection_id != Some(connection_id) {
            return Err("STALE_SFU_SESSION".into());
        }
        let Some(pending) = self.pending_commands.remove(&result.request_id) else {
            log::debug!(
                "SFU command result deduplicated: instance_id={instance_id} connection_id={connection_id} request_id={}",
                result.request_id
            );
            return Ok(());
        };
        match &result.result {
            Ok(_) => log::info!(
                "SFU command result: instance_id={instance_id} connection_id={connection_id} request_id={} result=OK",
                result.request_id
            ),
            Err(error) => log::warn!(
                "SFU command result: instance_id={instance_id} connection_id={connection_id} request_id={} result=ERR reason={}",
                result.request_id,
                error.reason
            ),
        }
        self.unacknowledged_commands.remove(&result.request_id);
        self.command_workers.remove(&result.request_id);
        match pending {
            PendingCommand::SyncRoom {
                instance_id: expected_instance,
                room_id,
                assignment_epoch,
                replay,
            } => {
                let valid = matches!(
                    &result.result,
                    Ok(sfu::CommandOk::RoomSynced(synced))
                        if synced.room_id == room_id
                            && synced.assignment_epoch == assignment_epoch
                );
                if expected_instance != instance_id || !valid {
                    self.fail_room(room_id, "WORKER_UNAVAILABLE".into());
                } else {
                    for command in replay {
                        self.actions.push_back(Action::Sfu(sfu::Output::Command {
                            connection_id,
                            command,
                        }));
                    }
                }
            }
            PendingCommand::UpgradeJoin {
                instance_id: expected_instance,
                room_id,
                client_id,
                lifecycle_id,
                assignment_epoch,
            } => {
                let valid = matches!(
                    &result.result,
                    Ok(sfu::CommandOk::MemberJoined(joined))
                        if joined.room_id == room_id
                            && joined.client_id == client_id
                            && joined.lifecycle_id == lifecycle_id
                            && joined.assignment_epoch == assignment_epoch
                );
                if expected_instance != instance_id || !valid {
                    let reason = result.result.err().map_or_else(
                        || "INVALID_COMMAND_RESULT".to_string(),
                        |error| error.reason,
                    );
                    self.abort_upgrade(room_id, reason)?;
                    return Ok(());
                }
                let room = self.rooms.get_mut(&room_id).ok_or("ROOM_NOT_FOUND")?;
                let upgrade = room.upgrade.as_mut().ok_or("ROOM_TRANSITION")?;
                upgrade.joined_clients.push(client_id);
                upgrade.next_client += 1;
                if upgrade.next_client == upgrade.ordered_clients.len() {
                    self.commit_upgrade(room_id)?;
                } else {
                    self.issue_next_upgrade_join(room_id)?;
                }
            }
            PendingCommand::SfuJoin {
                instance_id: expected_instance,
                room_id,
                client_id,
                lifecycle_id,
                assignment_epoch,
            } => {
                let valid = matches!(
                    &result.result,
                    Ok(sfu::CommandOk::MemberJoined(joined))
                        if joined.room_id == room_id
                            && joined.client_id == client_id
                            && joined.lifecycle_id == lifecycle_id
                            && joined.assignment_epoch == assignment_epoch
                );
                let room = self.rooms.get_mut(&room_id).ok_or("ROOM_NOT_FOUND")?;
                let pending_join = room.pending_join.take().ok_or("ROOM_TRANSITION")?;
                if expected_instance == instance_id && valid {
                    if let Some(worker) = self.workers.get_mut(instance_id) {
                        worker.assigned_clients = worker.assigned_clients.saturating_add(1);
                    }
                    let member = room.members.get(&client_id).ok_or("CLIENT_NOT_FOUND")?;
                    self.actions.push_back(Action::AdmissionCompleted {
                        authority_request_id: pending_join.authority_request_id,
                        result: Ok(Admission {
                            mode: RoomMode::Sfu,
                            signal_epoch: room.signal_epoch,
                            admission_token: member.token.clone(),
                            is_initiator: None,
                        }),
                    });
                } else {
                    room.members.remove(&pending_join.client_id);
                    self.actions.push_back(Action::AdmissionCompleted {
                        authority_request_id: pending_join.authority_request_id,
                        result: Err(result
                            .result
                            .err()
                            .map_or_else(|| "INVALID_COMMAND_RESULT".into(), |error| error.reason)),
                    });
                }
            }
            PendingCommand::SfuLeave {
                instance_id: expected_instance,
                room_id,
                client_id,
                lifecycle_id,
                assignment_epoch,
            } => {
                let valid = matches!(
                    &result.result,
                    Ok(sfu::CommandOk::MemberLeft(left))
                        if left.room_id == room_id
                            && left.client_id == client_id
                            && left.lifecycle_id == lifecycle_id
                            && left.assignment_epoch == assignment_epoch
                );
                let room = self.rooms.get_mut(&room_id).ok_or("ROOM_NOT_FOUND")?;
                let pending_leave = room.pending_leave.take().ok_or("ROOM_TRANSITION")?;
                if expected_instance == instance_id && valid {
                    room.members.remove(&client_id);
                    if let Some(worker) = self.workers.get_mut(instance_id) {
                        worker.assigned_clients = worker.assigned_clients.saturating_sub(1);
                        if room.members.is_empty() {
                            worker.assigned_rooms.remove(&room_id);
                        }
                    }
                    if let Some(authority_request_id) = pending_leave.authority_request_id {
                        self.actions.push_back(Action::RemovalCompleted {
                            authority_request_id,
                            room_id,
                            client_id,
                            result: Ok(()),
                        });
                    }
                    if room.members.is_empty() {
                        self.rooms.remove(&room_id);
                    } else {
                        // A member left an SFU room; if it has shrunk to at most two
                        // members and is otherwise idle, start the downgrade dwell.
                        self.arm_downgrade(room_id, now);
                    }
                } else {
                    if let Some(authority_request_id) = pending_leave.authority_request_id {
                        self.actions.push_back(Action::RemovalCompleted {
                            authority_request_id,
                            room_id,
                            client_id,
                            result: Err(result.result.err().map_or_else(
                                || "INVALID_COMMAND_RESULT".into(),
                                |error| error.reason,
                            )),
                        });
                    }
                }
            }
            PendingCommand::Signal | PendingCommand::Cleanup => {}
        }
        Ok(())
    }

    fn issue_next_upgrade_join(&mut self, room_id: RoomId) -> Result<(), String> {
        let room = self.rooms.get(&room_id).ok_or("ROOM_NOT_FOUND")?;
        let upgrade = room.upgrade.as_ref().ok_or("ROOM_TRANSITION")?;
        let client_id = upgrade.ordered_clients[upgrade.next_client];
        let member = room.members.get(&client_id).ok_or("CLIENT_NOT_FOUND")?;
        let instance_id = room.assigned_instance.clone().ok_or("WORKER_UNAVAILABLE")?;
        let connection_id = self
            .workers
            .get(&instance_id)
            .and_then(|worker| worker.connection_id)
            .ok_or("WORKER_UNAVAILABLE")?;
        let join = sfu::JoinMember {
            room_id,
            client_id,
            lifecycle_id: member.lifecycle_id,
            assignment_epoch: room.assignment_epoch,
        };
        let request_id = self.next_command_id();
        self.queue_command(
            instance_id.clone(),
            connection_id,
            sfu::Command {
                request_id,
                command: sfu::CommandKind::Join(join.clone()),
            },
            PendingCommand::UpgradeJoin {
                instance_id,
                room_id,
                client_id,
                lifecycle_id: join.lifecycle_id,
                assignment_epoch: join.assignment_epoch,
            },
        );
        Ok(())
    }

    fn commit_upgrade(&mut self, room_id: RoomId) -> Result<(), String> {
        let room = self.rooms.get_mut(&room_id).ok_or("ROOM_NOT_FOUND")?;
        let upgrade = room.upgrade.take().ok_or("ROOM_TRANSITION")?;
        room.mode = RoomMode::Sfu;
        room.signal_epoch = room.signal_epoch.saturating_add(1);
        for member in room.members.values_mut() {
            member.queued_messages.clear();
            member.is_initiator = false;
        }
        let joining = room
            .members
            .get(&upgrade.joining_client_id)
            .ok_or("CLIENT_NOT_FOUND")?;
        let admission = Admission {
            mode: RoomMode::Sfu,
            signal_epoch: room.signal_epoch,
            admission_token: joining.token.clone(),
            is_initiator: None,
        };
        let existing_clients = room
            .members
            .keys()
            .copied()
            .filter(|client_id| *client_id != upgrade.joining_client_id)
            .collect();
        self.actions.push_back(Action::Upgraded {
            room_id,
            signal_epoch: room.signal_epoch,
            existing_clients,
        });
        self.actions.push_back(Action::AdmissionCompleted {
            authority_request_id: upgrade.authority_request_id,
            result: Ok(admission),
        });
        Ok(())
    }

    fn abort_upgrade(&mut self, room_id: RoomId, reason: String) -> Result<(), String> {
        let room = self.rooms.get_mut(&room_id).ok_or("ROOM_NOT_FOUND")?;
        let upgrade = room.upgrade.take().ok_or("ROOM_TRANSITION")?;
        room.members.remove(&upgrade.joining_client_id);
        room.mode = RoomMode::P2p;
        let instance_id = room.assigned_instance.take().ok_or("WORKER_UNAVAILABLE")?;
        let assignment_epoch = room.assignment_epoch;
        let connection_id = self
            .workers
            .get(&instance_id)
            .and_then(|worker| worker.connection_id);
        if let Some(worker) = self.workers.get_mut(&instance_id) {
            worker.assigned_rooms.remove(&room_id);
            worker.assigned_clients = worker.assigned_clients.saturating_sub(3);
        }
        if let Some(connection_id) = connection_id {
            for client_id in upgrade.joined_clients {
                let lifecycle_id = self
                    .rooms
                    .get(&room_id)
                    .and_then(|room| room.members.get(&client_id))
                    .map(|member| member.lifecycle_id)
                    .ok_or("CLIENT_NOT_FOUND")?;
                let request_id = self.next_command_id();
                self.queue_command(
                    instance_id.clone(),
                    connection_id,
                    sfu::Command {
                        request_id,
                        command: sfu::CommandKind::Leave(sfu::LeaveMember {
                            room_id,
                            client_id,
                            lifecycle_id,
                            assignment_epoch,
                            reason: sfu::LeaveReason::RoomClosed,
                        }),
                    },
                    PendingCommand::Cleanup,
                );
            }
        }
        self.actions.push_back(Action::AdmissionCompleted {
            authority_request_id: upgrade.authority_request_id,
            result: Err(if reason.is_empty() {
                "WORKER_UNAVAILABLE".into()
            } else {
                reason
            }),
        });
        Ok(())
    }

    /// True when `room_id` is a stable SFU room small enough to run as direct P2P
    /// again: SFU mode, at most two members, and no membership transition or
    /// unfinished worker leave in flight.
    fn is_downgrade_eligible(&self, room_id: RoomId) -> bool {
        self.rooms.get(&room_id).is_some_and(|room| {
            room.mode == RoomMode::Sfu
                && !room.members.is_empty()
                && room.members.len() <= MAX_P2P_MEMBERS
                && room.upgrade.is_none()
                && room.pending_join.is_none()
                && room.pending_leave.is_none()
        })
    }

    /// Arm (or leave armed) the downgrade dwell for an SFU room that has shrunk to
    /// at most two members; clear it otherwise. Idempotent — an already-armed
    /// deadline is preserved so churn within the dwell does not keep pushing it out.
    fn arm_downgrade(&mut self, room_id: RoomId, now: Instant) {
        let eligible = self.is_downgrade_eligible(room_id);
        if let Some(room) = self.rooms.get_mut(&room_id) {
            if eligible {
                if room.downgrade_deadline.is_none() {
                    room.downgrade_deadline = Some(now + self.downgrade_dwell);
                }
            } else {
                room.downgrade_deadline = None;
            }
        }
    }

    /// Break-before-make SFU→P2P downgrade. Commits P2P and a new signal epoch,
    /// elects the lowest client id as the direct offerer, tears down every member's
    /// SFU leg on the worker, frees the assignment, and emits `Downgraded` so each
    /// browser is told to close its SFU peer connection and negotiate directly.
    fn commit_downgrade(&mut self, room_id: RoomId) -> Result<(), String> {
        let room = self.rooms.get_mut(&room_id).ok_or("ROOM_NOT_FOUND")?;
        room.mode = RoomMode::P2p;
        room.signal_epoch = room.signal_epoch.saturating_add(1);
        room.downgrade_deadline = None;
        let assignment_epoch = room.assignment_epoch;
        let instance_id = room.assigned_instance.take();

        let mut clients: Vec<ClientId> = room.members.keys().copied().collect();
        clients.sort_unstable();
        let initiator_client_id = *clients.first().ok_or("CLIENT_NOT_FOUND")?;
        let mut leaving = Vec::with_capacity(clients.len());
        for (&client_id, member) in room.members.iter_mut() {
            member.is_initiator = client_id == initiator_client_id;
            member.queued_messages.clear();
            leaving.push((client_id, member.lifecycle_id));
        }
        let signal_epoch = room.signal_epoch;

        // Tear down the SFU legs of every member and free the worker assignment.
        if let Some(instance_id) = instance_id {
            let connection_id = self
                .workers
                .get(&instance_id)
                .and_then(|worker| worker.connection_id);
            if let Some(worker) = self.workers.get_mut(&instance_id) {
                worker.assigned_rooms.remove(&room_id);
                worker.assigned_clients = worker.assigned_clients.saturating_sub(leaving.len());
            }
            if let Some(connection_id) = connection_id {
                for (client_id, lifecycle_id) in &leaving {
                    let request_id = self.next_command_id();
                    self.queue_command(
                        instance_id.clone(),
                        connection_id,
                        sfu::Command {
                            request_id,
                            command: sfu::CommandKind::Leave(sfu::LeaveMember {
                                room_id,
                                client_id: *client_id,
                                lifecycle_id: *lifecycle_id,
                                assignment_epoch,
                                reason: sfu::LeaveReason::RoomClosed,
                            }),
                        },
                        PendingCommand::Cleanup,
                    );
                }
            }
        }

        self.actions.push_back(Action::Downgraded {
            room_id,
            signal_epoch,
            initiator_client_id,
            clients,
        });
        Ok(())
    }

    fn fail_room(&mut self, room_id: RoomId, reason: String) {
        if let Some(room) = self.rooms.get_mut(&room_id) {
            room.mode = RoomMode::Failed;
            let clients = room.members.keys().copied().collect();
            self.actions.push_back(Action::RoomFailed {
                room_id,
                clients,
                reason,
            });
        }
    }

    fn select_worker(&self, additional_clients: usize) -> Option<String> {
        self.workers
            .iter()
            .filter(|(_, worker)| {
                let Some(health) = worker.health else {
                    return false;
                };
                worker.connection_id.is_some()
                    && health.state == sfu::State::Ready
                    && worker.assigned_rooms.len() < worker.capacity.max_rooms as usize
                    && worker.assigned_clients.saturating_add(additional_clients)
                        <= worker.capacity.max_clients as usize
            })
            .min_by_key(|(instance_id, worker)| {
                (
                    worker.assigned_clients,
                    worker.assigned_rooms.len(),
                    *instance_id,
                )
            })
            .map(|(instance_id, _)| instance_id.clone())
    }

    fn next_lifecycle(&mut self, room_id: RoomId, client_id: ClientId) -> u64 {
        let value = self.lifecycle_ids.entry((room_id, client_id)).or_default();
        *value = value.saturating_add(1).max(1);
        *value
    }

    fn next_command_id(&mut self) -> u64 {
        let request_id = self.next_sfu_request_id;
        self.next_sfu_request_id = self.next_sfu_request_id.saturating_add(1).max(1);
        request_id
    }

    fn queue_command(
        &mut self,
        instance_id: String,
        connection_id: sfu::ConnectionId,
        command: sfu::Command,
        pending: PendingCommand,
    ) {
        let (operation, room_id, client_id) = match &command.command {
            sfu::CommandKind::SyncRoom(c) => ("sync_room", c.room_id, 0),
            sfu::CommandKind::Join(c) => ("join", c.room_id, c.client_id),
            sfu::CommandKind::Leave(c) => ("leave", c.room_id, c.client_id),
            sfu::CommandKind::Signal(c) => ("signal", c.room_id, c.client_id),
        };
        // A signal command forwards the browser's opaque SDP/candidate to the worker; append it
        // (like a browser send) to show label the hop by type and expand
        // the SDP. Other commands carry no such body.
        let body = match &command.command {
            sfu::CommandKind::Signal(c) => format!("\n{}", c.message_json),
            _ => String::new(),
        };
        let request_id = command.request_id;
        log::info!(
            "SFU command: instance_id={instance_id} connection_id={connection_id} request_id={request_id} operation={operation} room_id={room_id} client_id={client_id}{body}"
        );
        self.pending_commands.insert(command.request_id, pending);
        self.command_workers.insert(command.request_id, instance_id);
        self.unacknowledged_commands
            .insert(command.request_id, command.clone());
        self.actions.push_back(Action::Sfu(sfu::Output::Command {
            connection_id,
            command,
        }));
    }

    pub fn remove(
        &mut self,
        authority_request_id: u64,
        room_id: RoomId,
        client_id: ClientId,
        token: &str,
    ) -> Result<RemovalResult, String> {
        let room = self.rooms.get_mut(&room_id).ok_or("ROOM_NOT_FOUND")?;
        let member = room.members.get(&client_id).ok_or("CLIENT_NOT_FOUND")?;
        if !constant_time_eq(member.token.as_bytes(), token.as_bytes()) {
            return Err("UNAUTHORIZED".into());
        }
        if room.mode == RoomMode::Upgrading
            || room.pending_join.is_some()
            || room.pending_leave.is_some()
        {
            return Err("ROOM_TRANSITION".into());
        }
        if room.mode == RoomMode::Sfu {
            let instance_id = room.assigned_instance.clone().ok_or("WORKER_UNAVAILABLE")?;
            let connection_id = self
                .workers
                .get(&instance_id)
                .and_then(|worker| worker.connection_id)
                .ok_or("WORKER_UNAVAILABLE")?;
            let lifecycle_id = member.lifecycle_id;
            let room = self.rooms.get_mut(&room_id).expect("room checked");
            room.pending_leave = Some(PendingLeave {
                authority_request_id: Some(authority_request_id),
                client_id,
            });
            let assignment_epoch = room.assignment_epoch;
            let request_id = self.next_command_id();
            self.queue_command(
                instance_id.clone(),
                connection_id,
                sfu::Command {
                    request_id,
                    command: sfu::CommandKind::Leave(sfu::LeaveMember {
                        room_id,
                        client_id,
                        lifecycle_id,
                        assignment_epoch,
                        reason: sfu::LeaveReason::User,
                    }),
                },
                PendingCommand::SfuLeave {
                    instance_id,
                    room_id,
                    client_id,
                    lifecycle_id,
                    assignment_epoch,
                },
            );
            return Ok(RemovalResult::Pending);
        }
        if room.mode == RoomMode::Failed {
            room.members.remove(&client_id);
            if room.members.is_empty() {
                self.rooms.remove(&room_id);
            }
            return Ok(RemovalResult::Complete(None));
        }
        room.members.remove(&client_id);
        let promotion = promote_survivor(room_id, room);
        if room.members.is_empty() {
            self.rooms.remove(&room_id);
        }
        Ok(RemovalResult::Complete(promotion))
    }

    pub fn occupancy(&self, room_id: RoomId) -> (usize, RoomMode) {
        self.rooms
            .get(&room_id)
            .map_or((0, RoomMode::P2p), |room| (room.members.len(), room.mode))
    }

    pub fn room_count(&self) -> usize {
        self.rooms.len()
    }

    pub fn member_count(&self) -> usize {
        self.rooms.values().map(|room| room.members.len()).sum()
    }

    pub fn registered_count(&self) -> usize {
        self.rooms
            .values()
            .flat_map(|room| room.members.values())
            .filter(|member| member.registered)
            .count()
    }

    pub fn connected_sfu_count(&self) -> usize {
        self.workers
            .values()
            .filter(|worker| worker.connection_id.is_some())
            .count()
    }

    pub fn ready_sfu_count(&self) -> usize {
        self.workers
            .values()
            .filter(|worker| {
                worker.connection_id.is_some()
                    && worker
                        .health
                        .is_some_and(|health| health.state == sfu::State::Ready)
            })
            .count()
    }

    pub fn handle_timeout(&mut self, now: Instant) -> Vec<Promotion> {
        let mut promotions = Vec::new();
        let expired_workers = self
            .workers
            .iter()
            .filter_map(|(instance_id, worker)| {
                worker
                    .disconnect_deadline
                    .filter(|deadline| *deadline <= now)
                    .map(|_| instance_id.clone())
            })
            .collect::<Vec<_>>();
        for instance_id in expired_workers {
            self.expire_worker(&instance_id);
        }
        let room_ids = self.rooms.keys().copied().collect::<Vec<_>>();
        for room_id in room_ids {
            // SFU→P2P downgrade: the ≤2-member dwell has expired. Re-check eligibility
            // (a third member may have joined during the dwell) before committing.
            let downgrade_due = self
                .rooms
                .get(&room_id)
                .and_then(|room| room.downgrade_deadline)
                .is_some_and(|deadline| deadline <= now);
            if downgrade_due {
                if self.is_downgrade_eligible(room_id) {
                    let _ = self.commit_downgrade(room_id);
                } else if let Some(room) = self.rooms.get_mut(&room_id) {
                    room.downgrade_deadline = None;
                }
                continue;
            }
            let expired_sfu_client = self.rooms.get(&room_id).and_then(|room| {
                (room.mode == RoomMode::Sfu && room.pending_leave.is_none())
                    .then(|| {
                        room.members.iter().find_map(|(&client_id, member)| {
                            member
                                .reconnect_deadline
                                .filter(|deadline| *deadline <= now)
                                .map(|_| client_id)
                        })
                    })
                    .flatten()
            });
            if let Some(client_id) = expired_sfu_client {
                // Retry the leave that couldn't be issued when the browser disconnected (worker
                // was unavailable then); re-arm the grace if it still can't go out.
                if !self.issue_sfu_leave(room_id, client_id, sfu::LeaveReason::Disconnected)
                    && let Some(member) = self
                        .rooms
                        .get_mut(&room_id)
                        .and_then(|room| room.members.get_mut(&client_id))
                {
                    member.reconnect_deadline = Some(now + self.reconnect_grace);
                }
                continue;
            }
            let Some(room) = self.rooms.get_mut(&room_id) else {
                continue;
            };
            if room.mode != RoomMode::P2p {
                continue;
            }
            let expired = room
                .members
                .iter()
                .filter_map(|(&client_id, member)| {
                    member
                        .reconnect_deadline
                        .filter(|deadline| *deadline <= now)
                        .map(|_| client_id)
                })
                .collect::<Vec<_>>();
            for client_id in &expired {
                room.members.remove(client_id);
            }
            // Only an actual removal can produce a survivor to promote. Without this guard
            // the sweep re-promotes the lone member of every idle one-member P2P room on
            // each timeout — including rooms nothing happened in — and the browser reads a
            // `p2p-promote` as "the other side left".
            if !expired.is_empty()
                && let Some(promotion) = promote_survivor(room_id, room)
            {
                promotions.push(promotion);
            }
            if room.members.is_empty() {
                self.rooms.remove(&room_id);
            }
        }
        promotions
    }

    pub fn poll_timeout(&self) -> Option<Instant> {
        let browser_timeout = self
            .rooms
            .values()
            .filter(|room| {
                room.mode == RoomMode::P2p
                    || (room.mode == RoomMode::Sfu && room.pending_leave.is_none())
            })
            .flat_map(|room| {
                room.members
                    .values()
                    .filter_map(|member| member.reconnect_deadline)
            })
            .min();
        let worker_timeout = self
            .workers
            .values()
            .filter_map(|worker| worker.disconnect_deadline)
            .min();
        let downgrade_timeout = self
            .rooms
            .values()
            .filter_map(|room| room.downgrade_deadline)
            .min();
        [browser_timeout, worker_timeout, downgrade_timeout]
            .into_iter()
            .flatten()
            .min()
    }

    pub fn poll_action(&mut self) -> Option<Action> {
        self.actions.pop_front()
    }

    pub fn clear(&mut self) {
        self.rooms.clear();
        self.workers.clear();
        self.pending_commands.clear();
        self.unacknowledged_commands.clear();
        self.command_workers.clear();
        self.actions.clear();
    }

    fn expire_worker(&mut self, instance_id: &str) {
        let assigned_rooms = self
            .workers
            .get(instance_id)
            .map(|worker| worker.assigned_rooms.iter().copied().collect::<Vec<_>>())
            .unwrap_or_default();
        for room_id in assigned_rooms {
            if self
                .rooms
                .get(&room_id)
                .is_some_and(|room| room.mode == RoomMode::Upgrading)
            {
                let _ = self.abort_upgrade(room_id, "WORKER_UNAVAILABLE".into());
                continue;
            }
            let Some(room) = self.rooms.get_mut(&room_id) else {
                continue;
            };
            if let Some(join) = room.pending_join.take() {
                room.members.remove(&join.client_id);
                self.actions.push_back(Action::AdmissionCompleted {
                    authority_request_id: join.authority_request_id,
                    result: Err("WORKER_UNAVAILABLE".into()),
                });
            }
            if let Some(leave) = room.pending_leave.take()
                && let Some(authority_request_id) = leave.authority_request_id
            {
                self.actions.push_back(Action::RemovalCompleted {
                    authority_request_id,
                    room_id,
                    client_id: leave.client_id,
                    result: Err("WORKER_UNAVAILABLE".into()),
                });
            }
            room.mode = RoomMode::Failed;
            self.actions.push_back(Action::RoomFailed {
                room_id,
                clients: room.members.keys().copied().collect(),
                reason: "WORKER_UNAVAILABLE".into(),
            });
        }
        let request_ids = self
            .command_workers
            .iter()
            .filter_map(|(&request_id, worker)| (worker == instance_id).then_some(request_id))
            .collect::<Vec<_>>();
        for request_id in request_ids {
            self.command_workers.remove(&request_id);
            self.unacknowledged_commands.remove(&request_id);
            self.pending_commands.remove(&request_id);
        }
        if let Some(worker) = self.workers.get_mut(instance_id) {
            worker.disconnect_deadline = None;
            worker.assigned_rooms.clear();
            worker.assigned_clients = 0;
        }
    }
}

fn new_member(
    now: Instant,
    reconnect_grace: Duration,
    token: String,
    is_initiator: bool,
    lifecycle_id: u64,
) -> Member {
    Member {
        token,
        is_initiator,
        lifecycle_id,
        registered: false,
        reconnect_deadline: Some(now + reconnect_grace),
        queued_messages: VecDeque::new(),
    }
}

fn push_queued(member: &mut Member, message: String) -> Result<(), String> {
    if member.queued_messages.len() >= MAX_QUEUED_MESSAGES {
        return Err("RESOURCE_EXHAUSTED".into());
    }
    member.queued_messages.push_back(message);
    Ok(())
}

fn command_room_id(command: &sfu::Command) -> Option<RoomId> {
    match &command.command {
        sfu::CommandKind::SyncRoom(command) => Some(command.room_id),
        sfu::CommandKind::Join(command) => Some(command.room_id),
        sfu::CommandKind::Leave(command) => Some(command.room_id),
        sfu::CommandKind::Signal(command) => Some(command.room_id),
    }
}

fn promote_survivor(room_id: RoomId, room: &mut Room) -> Option<Promotion> {
    if room.members.len() != 1 || room.mode != RoomMode::P2p {
        return None;
    }
    let (&client_id, member) = room.members.iter_mut().next()?;
    member.is_initiator = true;
    member.queued_messages.clear();
    member.registered.then_some(Promotion {
        room_id,
        client_id,
        signal_epoch: room.signal_epoch,
    })
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (&left, &right)| {
            difference | (left ^ right)
        })
        == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn register_ready_worker(rooms: &mut RoomTable) {
        rooms
            .handle_sfu_input(sfu::Input::Register {
                connection_id: 9,
                instance_id: "worker-1".into(),
                request_id: 1,
                capacity: sfu::Capacity {
                    max_rooms: 10,
                    max_clients: 100,
                },
            })
            .unwrap();
        assert!(matches!(
            rooms.poll_action(),
            Some(Action::Sfu(sfu::Output::Registered { .. }))
        ));
        rooms
            .handle_sfu_input(sfu::Input::Event {
                connection_id: 9,
                instance_id: "worker-1".into(),
                event: sfu::Event {
                    request_id: 2,
                    event: sfu::EventKind::Health(sfu::Health {
                        state: sfu::State::Ready,
                        capacity: sfu::Capacity {
                            max_rooms: 10,
                            max_clients: 100,
                        },
                        current_rooms: 0,
                        current_clients: 0,
                    }),
                },
            })
            .unwrap();
        assert!(matches!(
            rooms.poll_action(),
            Some(Action::Sfu(sfu::Output::EventAck { .. }))
        ));
    }

    #[test]
    fn admission_registration_relay_and_promotion() {
        let now = Instant::now();
        let mut rooms = RoomTable::new(Duration::from_secs(10));
        let first = match rooms.admit(1, now, 42, 101, "token-1".into()).unwrap() {
            AdmissionResult::Complete(value) => value,
            AdmissionResult::Pending => panic!("first join cannot be pending"),
        };
        let second = match rooms.admit(2, now, 42, 102, "token-2".into()).unwrap() {
            AdmissionResult::Complete(value) => value,
            AdmissionResult::Pending => panic!("second join cannot be pending"),
        };
        assert_eq!(first.is_initiator, Some(true));
        assert_eq!(second.is_initiator, Some(false));
        assert_eq!(
            rooms.admit(3, now, 42, 103, "token-3".into()),
            Err("NO_SFU_AVAILABLE".into())
        );
        rooms.register(42, 101, &first.admission_token).unwrap();
        rooms.register(42, 102, &second.admission_token).unwrap();
        assert!(matches!(
            rooms.send(42, 101, 0, "candidate".into()).unwrap(),
            SendResult::P2p(Some(Delivery { client_id: 102, .. }))
        ));
        let promotion = match rooms.remove(3, 42, 102, &second.admission_token).unwrap() {
            RemovalResult::Complete(Some(promotion)) => promotion,
            result => panic!("unexpected removal result: {result:?}"),
        };
        assert_eq!(promotion.client_id, 101);
    }

    /// A lone member waiting for a peer must never be promoted by a timeout. The sweep runs
    /// for every room whenever any deadline in the table fires, so an unrelated room's
    /// reconnect grace used to hand this room's only member a `p2p-promote`, which its
    /// browser renders as "the remote side hung up".
    #[test]
    fn idle_single_member_room_is_not_promoted_by_an_unrelated_timeout() {
        let now = Instant::now();
        let mut rooms = RoomTable::new(Duration::from_secs(10));

        // Room 7: two members, one of which disconnects and starts its reconnect grace.
        let leaving = match rooms.admit(1, now, 7, 701, "token-701".into()).unwrap() {
            AdmissionResult::Complete(value) => value,
            result => panic!("unexpected admission: {result:?}"),
        };
        let staying = match rooms.admit(2, now, 7, 702, "token-702".into()).unwrap() {
            AdmissionResult::Complete(value) => value,
            result => panic!("unexpected admission: {result:?}"),
        };
        rooms.register(7, 701, &leaving.admission_token).unwrap();
        rooms.register(7, 702, &staying.admission_token).unwrap();
        rooms.deregister(now, 7, 701);

        // Room 42: a single registered member waiting for someone to join.
        let alone = match rooms.admit(3, now, 42, 101, "token-101".into()).unwrap() {
            AdmissionResult::Complete(value) => value,
            result => panic!("unexpected admission: {result:?}"),
        };
        rooms.register(42, 101, &alone.admission_token).unwrap();

        // Room 7's grace expires. Its survivor is promoted; room 42 must be untouched.
        let promotions = rooms.handle_timeout(now + Duration::from_secs(11));
        assert_eq!(
            promotions.len(),
            1,
            "only room 7 may promote: {promotions:?}"
        );
        assert_eq!(promotions[0].room_id, 7);
        assert_eq!(promotions[0].client_id, 702);

        // And a later sweep with nothing pending promotes nobody at all.
        assert!(
            rooms
                .handle_timeout(now + Duration::from_secs(30))
                .is_empty(),
            "an idle sweep must not promote anyone"
        );
    }

    #[test]
    fn third_join_commits_only_after_all_worker_join_results() {
        let now = Instant::now();
        let mut rooms = RoomTable::new(Duration::from_secs(10));
        register_ready_worker(&mut rooms);
        for (request, client) in [(10, 101), (11, 102)] {
            assert!(matches!(
                rooms
                    .admit(request, now, 42, client, format!("token-{client}"))
                    .unwrap(),
                AdmissionResult::Complete(_)
            ));
        }
        assert_eq!(
            rooms.admit(12, now, 42, 103, "token-103".into()).unwrap(),
            AdmissionResult::Pending
        );
        for expected_client in [101, 102, 103] {
            let command = match rooms.poll_action().unwrap() {
                Action::Sfu(sfu::Output::Command { command, .. }) => command,
                action => panic!("unexpected action: {action:?}"),
            };
            let join = match command.command {
                sfu::CommandKind::Join(join) => join,
                _ => panic!("expected join command"),
            };
            assert_eq!(join.client_id, expected_client);
            rooms
                .handle_sfu_input(sfu::Input::CommandResult {
                    connection_id: 9,
                    instance_id: "worker-1".into(),
                    result: sfu::CommandResult {
                        request_id: command.request_id,
                        result: Ok(sfu::CommandOk::MemberJoined(join)),
                    },
                    now,
                })
                .unwrap();
        }
        assert!(matches!(
            rooms.poll_action(),
            Some(Action::Upgraded {
                signal_epoch: 1,
                ..
            })
        ));
        assert!(matches!(
            rooms.poll_action(),
            Some(Action::AdmissionCompleted {
                authority_request_id: 12,
                result: Ok(Admission {
                    mode: RoomMode::Sfu,
                    is_initiator: None,
                    ..
                }),
            })
        ));
        assert_eq!(rooms.occupancy(42), (3, RoomMode::Sfu));

        assert_eq!(
            rooms.admit(13, now, 42, 104, "token-104".into()).unwrap(),
            AdmissionResult::Pending
        );
        let command = match rooms.poll_action().unwrap() {
            Action::Sfu(sfu::Output::Command { command, .. }) => command,
            action => panic!("unexpected action: {action:?}"),
        };
        let join = match command.command {
            sfu::CommandKind::Join(join) => join,
            _ => panic!("expected join command"),
        };
        rooms
            .handle_sfu_input(sfu::Input::CommandResult {
                connection_id: 9,
                instance_id: "worker-1".into(),
                result: sfu::CommandResult {
                    request_id: command.request_id,
                    result: Ok(sfu::CommandOk::MemberJoined(join)),
                },
                now,
            })
            .unwrap();
        assert!(matches!(
            rooms.poll_action(),
            Some(Action::AdmissionCompleted {
                authority_request_id: 13,
                result: Ok(Admission {
                    mode: RoomMode::Sfu,
                    ..
                }),
            })
        ));
        assert_eq!(rooms.occupancy(42), (4, RoomMode::Sfu));

        assert_eq!(
            rooms.remove(14, 42, 104, "token-104").unwrap(),
            RemovalResult::Pending
        );
        let command = match rooms.poll_action().unwrap() {
            Action::Sfu(sfu::Output::Command { command, .. }) => command,
            action => panic!("unexpected action: {action:?}"),
        };
        let leave = match command.command {
            sfu::CommandKind::Leave(leave) => leave,
            _ => panic!("expected leave command"),
        };
        rooms
            .handle_sfu_input(sfu::Input::CommandResult {
                connection_id: 9,
                instance_id: "worker-1".into(),
                result: sfu::CommandResult {
                    request_id: command.request_id,
                    result: Ok(sfu::CommandOk::MemberLeft(leave)),
                },
                now,
            })
            .unwrap();
        assert!(matches!(
            rooms.poll_action(),
            Some(Action::RemovalCompleted {
                authority_request_id: 14,
                result: Ok(()),
                ..
            })
        ));
        assert_eq!(rooms.occupancy(42), (3, RoomMode::Sfu));

        assert_eq!(
            rooms.send(42, 101, 1, r#"{"type":"candidate"}"#.into()),
            Ok(SendResult::Sfu)
        );
        let pending_signal = match rooms.poll_action().unwrap() {
            Action::Sfu(sfu::Output::Command { command, .. }) => command,
            action => panic!("unexpected signal action: {action:?}"),
        };
        rooms
            .handle_sfu_input(sfu::Input::Disconnected {
                connection_id: 9,
                instance_id: "worker-1".into(),
                now,
            })
            .unwrap();
        rooms
            .handle_sfu_input(sfu::Input::Register {
                connection_id: 10,
                instance_id: "worker-1".into(),
                request_id: 50,
                capacity: sfu::Capacity {
                    max_rooms: 10,
                    max_clients: 100,
                },
            })
            .unwrap();
        assert!(matches!(
            rooms.poll_action(),
            Some(Action::Sfu(sfu::Output::Registered { resumed: true, .. }))
        ));
        let sync = match rooms.poll_action().unwrap() {
            Action::Sfu(sfu::Output::Command { command, .. }) => command,
            action => panic!("unexpected reconnect action: {action:?}"),
        };
        let sync_room = match sync.command {
            sfu::CommandKind::SyncRoom(sync_room) => sync_room,
            _ => panic!("expected room synchronization"),
        };
        assert!(
            rooms.poll_action().is_none(),
            "replay must wait for RoomSynced"
        );
        rooms
            .handle_sfu_input(sfu::Input::CommandResult {
                connection_id: 10,
                instance_id: "worker-1".into(),
                result: sfu::CommandResult {
                    request_id: sync.request_id,
                    result: Ok(sfu::CommandOk::RoomSynced(sfu::RoomSynced {
                        room_id: sync_room.room_id,
                        assignment_epoch: sync_room.assignment_epoch,
                    })),
                },
                now,
            })
            .unwrap();
        assert!(matches!(
            rooms.poll_action(),
            Some(Action::Sfu(sfu::Output::Command { command, connection_id: 10 }))
                if command == pending_signal
        ));
        assert_eq!(rooms.occupancy(42), (3, RoomMode::Sfu));
    }

    #[test]
    fn worker_disconnect_grace_expires_assigned_rooms() {
        let now = Instant::now();
        let mut rooms = RoomTable::new(Duration::from_secs(10));
        register_ready_worker(&mut rooms);
        for (request, client) in [(10, 101), (11, 102)] {
            rooms
                .admit(request, now, 42, client, format!("token-{client}"))
                .unwrap();
        }
        rooms.admit(12, now, 42, 103, "token-103".into()).unwrap();
        for _ in 0..3 {
            let command = match rooms.poll_action().unwrap() {
                Action::Sfu(sfu::Output::Command { command, .. }) => command,
                action => panic!("unexpected action: {action:?}"),
            };
            let join = match command.command {
                sfu::CommandKind::Join(join) => join,
                _ => panic!("expected join"),
            };
            rooms
                .handle_sfu_input(sfu::Input::CommandResult {
                    connection_id: 9,
                    instance_id: "worker-1".into(),
                    result: sfu::CommandResult {
                        request_id: command.request_id,
                        result: Ok(sfu::CommandOk::MemberJoined(join)),
                    },
                    now,
                })
                .unwrap();
        }
        assert!(matches!(rooms.poll_action(), Some(Action::Upgraded { .. })));
        assert!(matches!(
            rooms.poll_action(),
            Some(Action::AdmissionCompleted { .. })
        ));
        rooms
            .handle_sfu_input(sfu::Input::Disconnected {
                connection_id: 9,
                instance_id: "worker-1".into(),
                now,
            })
            .unwrap();
        rooms.handle_timeout(now + Duration::from_secs(10));
        assert!(matches!(
            rooms.poll_action(),
            Some(Action::RoomFailed { room_id: 42, .. })
        ));
        assert_eq!(rooms.occupancy(42), (3, RoomMode::Failed));
    }

    #[test]
    fn sfu_member_disconnect_issues_immediate_leave() {
        let now = Instant::now();
        let mut rooms = RoomTable::new(Duration::from_secs(10));
        register_ready_worker(&mut rooms);
        let first = match rooms.admit(10, now, 42, 101, "token-101".into()).unwrap() {
            AdmissionResult::Complete(admission) => admission,
            result => panic!("unexpected admission: {result:?}"),
        };
        rooms.admit(11, now, 42, 102, "token-102".into()).unwrap();
        rooms.admit(12, now, 42, 103, "token-103".into()).unwrap();
        for _ in 0..3 {
            let command = match rooms.poll_action().unwrap() {
                Action::Sfu(sfu::Output::Command { command, .. }) => command,
                action => panic!("unexpected action: {action:?}"),
            };
            let join = match command.command {
                sfu::CommandKind::Join(join) => join,
                _ => panic!("expected join"),
            };
            rooms
                .handle_sfu_input(sfu::Input::CommandResult {
                    connection_id: 9,
                    instance_id: "worker-1".into(),
                    result: sfu::CommandResult {
                        request_id: command.request_id,
                        result: Ok(sfu::CommandOk::MemberJoined(join)),
                    },
                    now,
                })
                .unwrap();
        }
        assert_eq!(rooms.occupancy(42), (3, RoomMode::Sfu));

        // Register client 101's browser WebSocket, then drain the upgrade/registration actions.
        rooms.register(42, 101, &first.admission_token).unwrap();
        while rooms.poll_action().is_some() {}

        // The WebSocket drops (browser close/refresh). An SFU member must be left immediately —
        // no reconnect grace — so the worker stops forwarding its media right away.
        rooms.deregister(now, 42, 101);
        let command = match rooms.poll_action().unwrap() {
            Action::Sfu(sfu::Output::Command { command, .. }) => command,
            action => panic!("expected immediate leave command, got: {action:?}"),
        };
        let leave = match command.command {
            sfu::CommandKind::Leave(leave) => leave,
            other => panic!("expected leave, got: {other:?}"),
        };
        assert_eq!(leave.room_id, 42);
        assert_eq!(leave.client_id, 101);
        assert!(matches!(leave.reason, sfu::LeaveReason::Disconnected));
        assert!(rooms.poll_action().is_none());
    }

    #[test]
    fn sfu_room_downgrades_to_p2p_after_dwell_at_two_members() {
        let now = Instant::now();
        let mut rooms = RoomTable::new(Duration::from_secs(10));
        register_ready_worker(&mut rooms);
        let tokens = [(10, 101), (11, 102), (12, 103)].map(|(request, client)| {
            match rooms
                .admit(request, now, 42, client, format!("token-{client}"))
                .unwrap()
            {
                AdmissionResult::Complete(admission) => (client, admission.admission_token),
                AdmissionResult::Pending => (client, format!("token-{client}")),
            }
        });
        for _ in 0..3 {
            let command = match rooms.poll_action().unwrap() {
                Action::Sfu(sfu::Output::Command { command, .. }) => command,
                action => panic!("unexpected action: {action:?}"),
            };
            let join = match command.command {
                sfu::CommandKind::Join(join) => join,
                _ => panic!("expected join"),
            };
            rooms
                .handle_sfu_input(sfu::Input::CommandResult {
                    connection_id: 9,
                    instance_id: "worker-1".into(),
                    result: sfu::CommandResult {
                        request_id: command.request_id,
                        result: Ok(sfu::CommandOk::MemberJoined(join)),
                    },
                    now,
                })
                .unwrap();
        }
        assert_eq!(rooms.occupancy(42), (3, RoomMode::Sfu));
        // Register the members so they carry no reconnect deadline (as real SFU members do).
        for (client, token) in &tokens {
            rooms.register(42, *client, token).unwrap();
        }
        while rooms.poll_action().is_some() {}

        // The third member leaves; drive its MemberLeft so the room shrinks to two.
        assert_eq!(
            rooms.remove(14, 42, 103, "token-103").unwrap(),
            RemovalResult::Pending
        );
        let command = match rooms.poll_action().unwrap() {
            Action::Sfu(sfu::Output::Command { command, .. }) => command,
            action => panic!("unexpected action: {action:?}"),
        };
        let leave = match command.command {
            sfu::CommandKind::Leave(leave) => leave,
            _ => panic!("expected leave"),
        };
        rooms
            .handle_sfu_input(sfu::Input::CommandResult {
                connection_id: 9,
                instance_id: "worker-1".into(),
                result: sfu::CommandResult {
                    request_id: command.request_id,
                    result: Ok(sfu::CommandOk::MemberLeft(leave)),
                },
                now,
            })
            .unwrap();
        while rooms.poll_action().is_some() {}
        assert_eq!(rooms.occupancy(42), (2, RoomMode::Sfu));

        // The dwell is armed and does not fire early.
        assert!(rooms.poll_timeout().is_some());
        rooms.handle_timeout(now + Duration::from_secs(1));
        assert_eq!(
            rooms.occupancy(42),
            (2, RoomMode::Sfu),
            "downgrade must wait out the dwell"
        );
        assert!(rooms.poll_action().is_none());

        // After the dwell, the room downgrades to direct P2P.
        rooms.handle_timeout(now + DOWNGRADE_DWELL + Duration::from_secs(1));
        assert_eq!(rooms.occupancy(42), (2, RoomMode::P2p));

        let mut leaves = 0;
        let mut downgraded = false;
        while let Some(action) = rooms.poll_action() {
            match action {
                Action::Sfu(sfu::Output::Command { command, .. }) => {
                    assert!(matches!(command.command, sfu::CommandKind::Leave(_)));
                    leaves += 1;
                }
                Action::Downgraded {
                    room_id,
                    signal_epoch,
                    initiator_client_id,
                    mut clients,
                } => {
                    assert_eq!(room_id, 42);
                    assert_eq!(signal_epoch, 2, "P2P->SFU->P2P bumps the epoch twice");
                    assert_eq!(initiator_client_id, 101, "lowest client id offers");
                    clients.sort_unstable();
                    assert_eq!(clients, vec![101, 102]);
                    downgraded = true;
                }
                other => panic!("unexpected action after downgrade: {other:?}"),
            }
        }
        assert_eq!(leaves, 2, "both remaining members' SFU legs must be left");
        assert!(downgraded, "a Downgraded control must be emitted");

        // A fresh third join can upgrade the room again.
        assert_eq!(
            rooms.admit(20, now, 42, 104, "token-104".into()).unwrap(),
            AdmissionResult::Pending
        );
        assert!(matches!(rooms.occupancy(42), (3, RoomMode::Upgrading)));
    }
}
