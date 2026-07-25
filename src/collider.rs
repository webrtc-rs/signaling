//! Sans-I/O signaling authority for the V1 compatibility protocol and P2P V2.

use crate::client::ClientId;
use crate::messages::{
    Message, V2Downgrade, V2Promoted, V2Registered, V2RoomFailed, V2Upgrade, WsClientMsg,
    server_err, server_msg, to_wire,
};
use crate::room::RoomId;
use crate::room_table::RoomTable;
use crate::v2::{self, RoomTable as V2RoomTable};
use sansio::Protocol;
use std::collections::{HashMap, VecDeque};
use std::convert::Infallible;
use std::time::{Duration, Instant};

pub type ConnectionId = u64;
pub type RequestId = u64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BrowserInput {
    Connected {
        connection_id: ConnectionId,
    },
    Text {
        connection_id: ConnectionId,
        text: String,
        now: Instant,
    },
    Disconnected {
        connection_id: ConnectionId,
        now: Instant,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BrowserOutput {
    Text {
        connection_id: ConnectionId,
        text: String,
    },
    Close {
        connection_id: ConnectionId,
    },
    Sfu(crate::sfu::Output),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthorityOperation {
    Admit {
        roomid: RoomId,
        clientid: ClientId,
        is_loopback: bool,
        now: Instant,
    },
    Remove {
        roomid: RoomId,
        clientid: ClientId,
    },
    Occupancy {
        roomid: RoomId,
    },
    Inject {
        roomid: RoomId,
        clientid: ClientId,
        msg: String,
        now: Instant,
    },
    AdmitV2 {
        room_id: v2::RoomId,
        client_id: v2::ClientId,
        admission_token: String,
        now: Instant,
    },
    RemoveV2 {
        room_id: v2::RoomId,
        client_id: v2::ClientId,
        admission_token: String,
    },
    OccupancyV2 {
        room_id: v2::RoomId,
    },
    Status,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorityCommand {
    pub request_id: RequestId,
    pub operation: AuthorityOperation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusSnapshot {
    pub rooms: usize,
    pub v2_rooms: usize,
    pub clients: usize,
    pub websocket_connections: usize,
    pub total_websocket_connections: u64,
    pub websocket_errors: u64,
    pub connected_sfu_instances: usize,
    pub ready_sfu_instances: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthorityResult {
    Admitted {
        is_initiator: bool,
        messages: Vec<String>,
    },
    Removed,
    Occupancy {
        count: usize,
    },
    Injected,
    AdmittedV2 {
        mode: v2::RoomMode,
        signal_epoch: u64,
        admission_token: String,
        is_initiator: Option<bool>,
    },
    RemovedV2,
    OccupancyV2 {
        count: usize,
        mode: v2::RoomMode,
    },
    Status(StatusSnapshot),
    Error {
        result: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorityResponse {
    pub request_id: RequestId,
    pub result: AuthorityResult,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Session {
    Connected,
    RegisteredV1 {
        roomid: RoomId,
        clientid: ClientId,
    },
    RegisteredV2 {
        room_id: v2::RoomId,
        client_id: v2::ClientId,
    },
}

/// Owns all mutable room state. Drivers must serialize inputs through this value.
pub struct Collider {
    rooms: RoomTable,
    v2_rooms: V2RoomTable,
    sessions: HashMap<ConnectionId, Session>,
    connections: HashMap<(RoomId, ClientId), ConnectionId>,
    v2_connections: HashMap<(v2::RoomId, v2::ClientId), ConnectionId>,
    browser_outputs: VecDeque<BrowserOutput>,
    authority_responses: VecDeque<AuthorityResponse>,
    total_websocket_connections: u64,
    websocket_errors: u64,
}

impl Collider {
    pub fn new(register_timeout: Duration) -> Self {
        Self {
            rooms: RoomTable::new(register_timeout),
            v2_rooms: V2RoomTable::new(register_timeout),
            sessions: HashMap::new(),
            connections: HashMap::new(),
            v2_connections: HashMap::new(),
            browser_outputs: VecDeque::new(),
            authority_responses: VecDeque::new(),
            total_websocket_connections: 0,
            websocket_errors: 0,
        }
    }

    /// Override the SFU→P2P downgrade dwell (default 2 seconds). Applies to V2 rooms only; V1 has
    /// no SFU mode.
    pub fn set_downgrade_dwell(&mut self, dwell: Duration) {
        self.v2_rooms.set_downgrade_dwell(dwell);
    }

    fn handle_browser_text(
        &mut self,
        connection_id: ConnectionId,
        text: String,
        now: Instant,
    ) -> Result<(), String> {
        let value: serde_json::Value = match serde_json::from_str(&text) {
            Ok(value) => value,
            Err(error) => {
                self.fail_connection(
                    connection_id,
                    format!("websocket.JSON.Receive error: {error}"),
                );
                return Ok(());
            }
        };
        let msg: WsClientMsg = match serde_json::from_value(value) {
            Ok(msg) => msg,
            Err(error) => {
                self.fail_connection(
                    connection_id,
                    format!("websocket.JSON.Receive error: {error}"),
                );
                return Ok(());
            }
        };

        match msg.cmd.as_str() {
            "register" if msg.ver == Some(2) => self.register_v2(connection_id, msg),
            "register" if msg.ver.is_none() => self.register_v1(connection_id, msg, now),
            "register" => {
                self.fail_connection(connection_id, "INVALID_VERSION");
                Ok(())
            }
            "send" => self.send(connection_id, msg, now),
            _ => {
                self.fail_connection(connection_id, "Invalid message: unexpected 'cmd'");
                Ok(())
            }
        }
    }

    fn register_v1(
        &mut self,
        connection_id: ConnectionId,
        msg: WsClientMsg,
        now: Instant,
    ) -> Result<(), String> {
        if matches!(
            self.sessions.get(&connection_id),
            Some(Session::RegisteredV1 { .. } | Session::RegisteredV2 { .. })
        ) {
            self.fail_connection(connection_id, "Duplicated register request");
            return Ok(());
        }
        if msg.roomid.is_empty() || msg.clientid.is_empty() {
            self.fail_connection(
                connection_id,
                "Invalid register request: missing 'clientid' or 'roomid'",
            );
            return Ok(());
        }

        match self.rooms.register(now, &msg.roomid, &msg.clientid) {
            Ok(()) => {
                log::info!(
                    "V1 register: connection_id={connection_id} room_id={} client_id={}",
                    msg.roomid,
                    msg.clientid
                );
                self.sessions.insert(
                    connection_id,
                    Session::RegisteredV1 {
                        roomid: msg.roomid.clone(),
                        clientid: msg.clientid.clone(),
                    },
                );
                self.connections
                    .insert((msg.roomid, msg.clientid), connection_id);
                self.total_websocket_connections =
                    self.total_websocket_connections.saturating_add(1);
                self.drain_room_writes();
            }
            Err(error) => self.fail_connection(connection_id, error),
        }
        Ok(())
    }

    fn register_v2(&mut self, connection_id: ConnectionId, msg: WsClientMsg) -> Result<(), String> {
        if matches!(
            self.sessions.get(&connection_id),
            Some(Session::RegisteredV1 { .. } | Session::RegisteredV2 { .. })
        ) {
            self.fail_connection(connection_id, "Duplicated register request");
            return Ok(());
        }
        let room_id = match canonical_u64(&msg.roomid) {
            Some(value) => value,
            None => {
                self.fail_connection(connection_id, "INVALID_ROOM_ID");
                return Ok(());
            }
        };
        let client_id = match canonical_u64(&msg.clientid) {
            Some(value) => value,
            None => {
                self.fail_connection(connection_id, "INVALID_CLIENT_ID");
                return Ok(());
            }
        };
        if msg.token.is_empty() {
            self.fail_connection(connection_id, "UNAUTHORIZED");
            return Ok(());
        }
        let registration = match self.v2_rooms.register(room_id, client_id, &msg.token) {
            Ok(registration) => registration,
            Err(error) => {
                self.fail_connection(connection_id, error);
                return Ok(());
            }
        };
        log::info!(
            "V2 register: connection_id={connection_id} room_id={room_id} client_id={client_id} epoch={}",
            registration.signal_epoch
        );
        self.sessions
            .insert(connection_id, Session::RegisteredV2 { room_id, client_id });
        self.v2_connections
            .insert((room_id, client_id), connection_id);
        self.total_websocket_connections = self.total_websocket_connections.saturating_add(1);

        // The authoritative snapshot must precede any queued SDP/ICE messages.
        log::info!(
            "V2 control: control=registered connection_id={connection_id} room_id={room_id} client_id={client_id} epoch={} mode={}",
            registration.signal_epoch,
            room_mode_name(registration.mode)
        );
        self.browser_outputs.push_back(BrowserOutput::Text {
            connection_id,
            text: to_wire(&V2Registered {
                control: "registered",
                roomid: room_id.to_string(),
                epoch: registration.signal_epoch.to_string(),
                mode: room_mode_name(registration.mode),
                is_initiator: registration.is_initiator,
            }),
        });
        // Early P2P signaling from the peer (typically the initiator's offer and its candidates)
        // was queued before this client registered; it is delivered now. Log each body to show
        // it (the same "V2 deliver" the relay path logs).
        for message in registration.queued_messages {
            log::info!(
                "V2 deliver: connection_id={connection_id} room_id={room_id} client_id={client_id} bytes={}\n{}",
                message.len(),
                message
            );
            self.browser_outputs.push_back(BrowserOutput::Text {
                connection_id,
                text: server_msg(&message),
            });
        }
        Ok(())
    }

    fn send(
        &mut self,
        connection_id: ConnectionId,
        msg: WsClientMsg,
        now: Instant,
    ) -> Result<(), String> {
        let Some(session) = self.sessions.get(&connection_id).cloned() else {
            self.fail_connection(connection_id, "Client not registered");
            return Ok(());
        };
        if msg.msg.is_empty() {
            self.fail_connection(connection_id, "Invalid send request: missing 'msg'");
            return Ok(());
        }

        match session {
            Session::RegisteredV1 { roomid, clientid } => {
                log::info!(
                    "V1 send: connection_id={connection_id} room_id={roomid} client_id={clientid} bytes={}",
                    msg.msg.len()
                );
                self.rooms.handle_read(Message {
                    roomid,
                    clientid,
                    msg: msg.msg,
                })?;
                while let Some(message) = self.rooms.poll_read() {
                    self.rooms
                        .send(now, &message.roomid, &message.clientid, message.msg)?;
                }
                self.drain_room_writes();
            }
            Session::RegisteredV2 { room_id, client_id } => {
                let Some(signal_epoch) = msg.epoch.as_ref().and_then(canonical_json_u64) else {
                    log::info!(
                        "V2 send dropped: connection_id={connection_id} room_id={room_id} client_id={client_id} reason=invalid_epoch"
                    );
                    return Ok(());
                };
                // The full body follows on the next line to show the
                // SDP/candidate under a click-to-expand [+]. signaling never parses this payload.
                log::info!(
                    "V2 send: connection_id={connection_id} room_id={room_id} client_id={client_id} epoch={signal_epoch} bytes={}\n{}",
                    msg.msg.len(),
                    msg.msg
                );
                if let v2::SendResult::P2p(Some(delivery)) =
                    self.v2_rooms
                        .send(room_id, client_id, signal_epoch, msg.msg)?
                    && let Some(&peer_connection) = self
                        .v2_connections
                        .get(&(delivery.room_id, delivery.client_id))
                {
                    log::info!(
                        "V2 deliver: connection_id={peer_connection} room_id={} client_id={} bytes={}\n{}",
                        delivery.room_id,
                        delivery.client_id,
                        delivery.message.len(),
                        delivery.message
                    );
                    self.browser_outputs.push_back(BrowserOutput::Text {
                        connection_id: peer_connection,
                        text: server_msg(&delivery.message),
                    });
                }
                self.drain_v2_actions();
            }
            Session::Connected => {
                self.fail_connection(connection_id, "Client not registered");
            }
        }
        Ok(())
    }

    fn disconnect(&mut self, connection_id: ConnectionId, now: Instant) {
        let Some(session) = self.sessions.remove(&connection_id) else {
            return;
        };
        match session {
            Session::RegisteredV1 { roomid, clientid } => {
                let key = (roomid.clone(), clientid.clone());
                if self.connections.get(&key) == Some(&connection_id) {
                    self.connections.remove(&key);
                    self.rooms.deregister(now, &roomid, &clientid);
                }
            }
            Session::RegisteredV2 { room_id, client_id } => {
                let key = (room_id, client_id);
                if self.v2_connections.get(&key) == Some(&connection_id) {
                    self.v2_connections.remove(&key);
                    self.v2_rooms.deregister(now, room_id, client_id);
                    // An SFU disconnect issues an immediate `Leave` command; flush it to the
                    // worker so its forwarded media stops right away.
                    self.drain_v2_actions();
                }
            }
            Session::Connected => {}
        }
    }

    fn fail_connection(&mut self, connection_id: ConnectionId, error: impl Into<String>) {
        self.websocket_errors = self.websocket_errors.saturating_add(1);
        self.browser_outputs.push_back(BrowserOutput::Text {
            connection_id,
            text: server_err(&error.into()),
        });
        self.browser_outputs
            .push_back(BrowserOutput::Close { connection_id });
    }

    fn close_client_connection(&mut self, roomid: &RoomId, clientid: &ClientId) {
        let key = (roomid.clone(), clientid.clone());
        if let Some(connection_id) = self.connections.remove(&key) {
            self.sessions.remove(&connection_id);
            self.browser_outputs
                .push_back(BrowserOutput::Close { connection_id });
        }
    }

    fn close_v2_client_connection(&mut self, room_id: v2::RoomId, client_id: v2::ClientId) {
        if let Some(connection_id) = self.v2_connections.remove(&(room_id, client_id)) {
            self.sessions.remove(&connection_id);
            self.browser_outputs
                .push_back(BrowserOutput::Close { connection_id });
        }
    }

    fn push_promotion(&mut self, promotion: v2::Promotion) {
        if let Some(&connection_id) = self
            .v2_connections
            .get(&(promotion.room_id, promotion.client_id))
        {
            log::info!(
                "V2 control: control=p2p-promote connection_id={connection_id} room_id={} client_id={} epoch={}",
                promotion.room_id,
                promotion.client_id,
                promotion.signal_epoch
            );
            self.browser_outputs.push_back(BrowserOutput::Text {
                connection_id,
                text: to_wire(&V2Promoted {
                    control: "p2p-promote",
                    roomid: promotion.room_id.to_string(),
                    epoch: promotion.signal_epoch.to_string(),
                    is_initiator: true,
                }),
            });
        }
    }

    fn drain_room_writes(&mut self) {
        while let Some(message) = self.rooms.poll_write() {
            if let Some(&connection_id) = self
                .connections
                .get(&(message.roomid.clone(), message.clientid.clone()))
            {
                self.browser_outputs.push_back(BrowserOutput::Text {
                    connection_id,
                    text: server_msg(&message.msg),
                });
            }
        }
    }

    fn handle_authority(&mut self, command: AuthorityCommand) {
        let result = match command.operation {
            AuthorityOperation::Admit {
                roomid,
                clientid,
                is_loopback,
                now,
            } => match self.rooms.join(now, &roomid, &clientid, is_loopback) {
                Ok((is_initiator, messages)) => AuthorityResult::Admitted {
                    is_initiator,
                    messages,
                },
                Err(result) => AuthorityResult::Error { result },
            },
            AuthorityOperation::Remove { roomid, clientid } => {
                self.close_client_connection(&roomid, &clientid);
                self.rooms.leave(&roomid, &clientid);
                AuthorityResult::Removed
            }
            AuthorityOperation::Occupancy { roomid } => AuthorityResult::Occupancy {
                count: self.rooms.occupancy(&roomid),
            },
            AuthorityOperation::Inject {
                roomid,
                clientid,
                msg,
                now,
            } => match self.rooms.save_or_send(now, &roomid, &clientid, msg) {
                Ok(()) => {
                    self.drain_room_writes();
                    AuthorityResult::Injected
                }
                Err(result) => AuthorityResult::Error { result },
            },
            AuthorityOperation::AdmitV2 {
                room_id,
                client_id,
                admission_token,
                now,
            } => match self.v2_rooms.admit(
                command.request_id,
                now,
                room_id,
                client_id,
                admission_token,
            ) {
                Ok(v2::AdmissionResult::Complete(admission)) => AuthorityResult::AdmittedV2 {
                    mode: admission.mode,
                    signal_epoch: admission.signal_epoch,
                    admission_token: admission.admission_token,
                    is_initiator: admission.is_initiator,
                },
                Ok(v2::AdmissionResult::Pending) => {
                    self.drain_v2_actions();
                    return;
                }
                Err(result) => AuthorityResult::Error { result },
            },
            AuthorityOperation::RemoveV2 {
                room_id,
                client_id,
                admission_token,
            } => {
                match self
                    .v2_rooms
                    .remove(command.request_id, room_id, client_id, &admission_token)
                {
                    Ok(v2::RemovalResult::Complete(promotion)) => {
                        self.close_v2_client_connection(room_id, client_id);
                        if let Some(promotion) = promotion {
                            self.push_promotion(promotion);
                        }
                        AuthorityResult::RemovedV2
                    }
                    Ok(v2::RemovalResult::Pending) => {
                        self.drain_v2_actions();
                        return;
                    }
                    Err(result) => AuthorityResult::Error { result },
                }
            }
            AuthorityOperation::OccupancyV2 { room_id } => {
                let (count, mode) = self.v2_rooms.occupancy(room_id);
                AuthorityResult::OccupancyV2 { count, mode }
            }
            AuthorityOperation::Status => AuthorityResult::Status(StatusSnapshot {
                rooms: self.rooms.room_count(),
                v2_rooms: self.v2_rooms.room_count(),
                clients: self.rooms.client_count() + self.v2_rooms.member_count(),
                websocket_connections: self.rooms.ws_count() + self.v2_rooms.registered_count(),
                total_websocket_connections: self.total_websocket_connections,
                websocket_errors: self.websocket_errors,
                connected_sfu_instances: self.v2_rooms.connected_sfu_count(),
                ready_sfu_instances: self.v2_rooms.ready_sfu_count(),
            }),
        };
        self.authority_responses.push_back(AuthorityResponse {
            request_id: command.request_id,
            result,
        });
        self.drain_v2_actions();
    }

    pub fn handle_sfu_input(&mut self, input: crate::sfu::Input) -> Result<(), String> {
        self.v2_rooms.handle_sfu_input(input)?;
        self.drain_v2_actions();
        Ok(())
    }

    fn drain_v2_actions(&mut self) {
        while let Some(action) = self.v2_rooms.poll_action() {
            match action {
                v2::Action::Sfu(output) => {
                    self.browser_outputs.push_back(BrowserOutput::Sfu(output));
                }
                v2::Action::AdmissionCompleted {
                    authority_request_id,
                    result,
                } => {
                    let result = match result {
                        Ok(admission) => AuthorityResult::AdmittedV2 {
                            mode: admission.mode,
                            signal_epoch: admission.signal_epoch,
                            admission_token: admission.admission_token,
                            is_initiator: admission.is_initiator,
                        },
                        Err(result) => AuthorityResult::Error { result },
                    };
                    self.authority_responses.push_back(AuthorityResponse {
                        request_id: authority_request_id,
                        result,
                    });
                }
                v2::Action::RemovalCompleted {
                    authority_request_id,
                    room_id,
                    client_id,
                    result,
                } => {
                    let result = match result {
                        Ok(()) => {
                            self.close_v2_client_connection(room_id, client_id);
                            AuthorityResult::RemovedV2
                        }
                        Err(result) => AuthorityResult::Error { result },
                    };
                    self.authority_responses.push_back(AuthorityResponse {
                        request_id: authority_request_id,
                        result,
                    });
                }
                v2::Action::Upgraded {
                    room_id,
                    signal_epoch,
                    existing_clients,
                } => {
                    for client_id in existing_clients {
                        if let Some(&connection_id) = self.v2_connections.get(&(room_id, client_id))
                        {
                            log::info!(
                                "V2 control: control=sfu-upgrade connection_id={connection_id} room_id={room_id} client_id={client_id} epoch={signal_epoch}"
                            );
                            self.browser_outputs.push_back(BrowserOutput::Text {
                                connection_id,
                                text: to_wire(&V2Upgrade {
                                    control: "sfu-upgrade",
                                    roomid: room_id.to_string(),
                                    epoch: signal_epoch.to_string(),
                                }),
                            });
                        }
                    }
                }
                v2::Action::Downgraded {
                    room_id,
                    signal_epoch,
                    initiator_client_id,
                    clients,
                } => {
                    for client_id in clients {
                        if let Some(&connection_id) = self.v2_connections.get(&(room_id, client_id))
                        {
                            log::info!(
                                "V2 control: control=sfu-downgrade connection_id={connection_id} room_id={room_id} client_id={client_id} epoch={signal_epoch} is_initiator={}",
                                client_id == initiator_client_id
                            );
                            self.browser_outputs.push_back(BrowserOutput::Text {
                                connection_id,
                                text: to_wire(&V2Downgrade {
                                    control: "sfu-downgrade",
                                    roomid: room_id.to_string(),
                                    epoch: signal_epoch.to_string(),
                                    is_initiator: client_id == initiator_client_id,
                                }),
                            });
                        }
                    }
                }
                v2::Action::Deliver(delivery) => {
                    if let Some(&connection_id) = self
                        .v2_connections
                        .get(&(delivery.room_id, delivery.client_id))
                    {
                        log::info!(
                            "V2 deliver: connection_id={connection_id} room_id={} client_id={} bytes={}\n{}",
                            delivery.room_id,
                            delivery.client_id,
                            delivery.message.len(),
                            delivery.message
                        );
                        self.browser_outputs.push_back(BrowserOutput::Text {
                            connection_id,
                            text: server_msg(&delivery.message),
                        });
                    }
                }
                v2::Action::RoomFailed {
                    room_id,
                    clients,
                    reason,
                } => {
                    for client_id in clients {
                        if let Some(&connection_id) = self.v2_connections.get(&(room_id, client_id))
                        {
                            log::info!(
                                "V2 control: control=room-failed connection_id={connection_id} room_id={room_id} client_id={client_id} reason={reason}"
                            );
                            self.browser_outputs.push_back(BrowserOutput::Text {
                                connection_id,
                                text: to_wire(&V2RoomFailed {
                                    control: "room-failed",
                                    roomid: room_id.to_string(),
                                    reason: reason.clone(),
                                }),
                            });
                        }
                    }
                }
            }
        }
    }
}

impl Protocol<BrowserInput, AuthorityCommand, Infallible> for Collider {
    type Rout = Infallible;
    type Wout = BrowserOutput;
    type Eout = AuthorityResponse;
    type Error = String;
    type Time = Instant;

    fn handle_read(&mut self, input: BrowserInput) -> Result<(), Self::Error> {
        match input {
            BrowserInput::Connected { connection_id } => {
                self.sessions.insert(connection_id, Session::Connected);
            }
            BrowserInput::Text {
                connection_id,
                text,
                now,
            } => self.handle_browser_text(connection_id, text, now)?,
            BrowserInput::Disconnected { connection_id, now } => {
                self.disconnect(connection_id, now)
            }
        }
        Ok(())
    }

    fn poll_read(&mut self) -> Option<Self::Rout> {
        None
    }

    fn handle_write(&mut self, command: AuthorityCommand) -> Result<(), Self::Error> {
        self.handle_authority(command);
        Ok(())
    }

    fn poll_write(&mut self) -> Option<Self::Wout> {
        self.browser_outputs.pop_front()
    }

    fn poll_event(&mut self) -> Option<Self::Eout> {
        self.authority_responses.pop_front()
    }

    fn handle_event(&mut self, event: Infallible) -> Result<(), Self::Error> {
        match event {}
    }

    fn handle_timeout(&mut self, now: Self::Time) -> Result<(), Self::Error> {
        self.rooms.handle_timeout(now)?;
        for promotion in self.v2_rooms.handle_timeout(now) {
            self.push_promotion(promotion);
        }
        // Timeouts can queue SFU commands (e.g. a retried disconnect `Leave`); flush them so they
        // reach the worker instead of stalling until the next browser/authority event.
        self.drain_v2_actions();
        Ok(())
    }

    fn poll_timeout(&mut self) -> Option<Self::Time> {
        match (self.rooms.poll_timeout(), self.v2_rooms.poll_timeout()) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (left, right) => left.or(right),
        }
    }

    fn close(&mut self) -> Result<(), Self::Error> {
        for connection_id in self.sessions.keys().copied().collect::<Vec<_>>() {
            self.browser_outputs
                .push_back(BrowserOutput::Close { connection_id });
        }
        self.sessions.clear();
        self.connections.clear();
        self.v2_connections.clear();
        self.v2_rooms.clear();
        self.rooms.close()
    }
}

fn canonical_u64(value: &str) -> Option<u64> {
    if value == "0" {
        return Some(0);
    }
    if value.is_empty()
        || value.starts_with('0')
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    value.parse().ok()
}

fn canonical_json_u64(value: &serde_json::Value) -> Option<u64> {
    value.as_str().and_then(canonical_u64)
}

fn room_mode_name(mode: v2::RoomMode) -> &'static str {
    match mode {
        v2::RoomMode::P2p => "p2p",
        v2::RoomMode::Upgrading => "upgrading",
        v2::RoomMode::Sfu => "sfu",
        v2::RoomMode::Failed => "failed",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(collider: &mut Collider, connection_id: u64, value: &str, now: Instant) {
        collider
            .handle_read(BrowserInput::Text {
                connection_id,
                text: value.to_string(),
                now,
            })
            .unwrap();
    }

    fn connect(collider: &mut Collider, connection_id: u64) {
        collider
            .handle_read(BrowserInput::Connected { connection_id })
            .unwrap();
    }

    fn authority(
        collider: &mut Collider,
        request_id: u64,
        operation: AuthorityOperation,
    ) -> AuthorityResult {
        collider
            .handle_write(AuthorityCommand {
                request_id,
                operation,
            })
            .unwrap();
        let response = collider.poll_event().unwrap();
        assert_eq!(response.request_id, request_id);
        response.result
    }

    fn assert_error_and_close(collider: &mut Collider, connection_id: ConnectionId, error: &str) {
        assert_eq!(
            collider.poll_write(),
            Some(BrowserOutput::Text {
                connection_id,
                text: server_err(error),
            })
        );
        assert_eq!(
            collider.poll_write(),
            Some(BrowserOutput::Close { connection_id })
        );
    }

    #[test]
    fn successful_v1_registration_is_silent_and_send_relays() {
        let now = Instant::now();
        let mut collider = Collider::new(Duration::from_secs(10));
        for (request_id, clientid) in [(1, "1"), (2, "2")] {
            assert!(matches!(
                authority(
                    &mut collider,
                    request_id,
                    AuthorityOperation::Admit {
                        roomid: "room".into(),
                        clientid: clientid.into(),
                        is_loopback: false,
                        now,
                    }
                ),
                AuthorityResult::Admitted { .. }
            ));
        }
        connect(&mut collider, 10);
        connect(&mut collider, 20);
        text(
            &mut collider,
            10,
            r#"{"cmd":"register","roomid":"room","clientid":"1"}"#,
            now,
        );
        text(
            &mut collider,
            20,
            r#"{"cmd":"register","roomid":"room","clientid":"2"}"#,
            now,
        );
        assert_eq!(collider.poll_write(), None);

        text(
            &mut collider,
            10,
            r#"{"cmd":"send","msg":"candidate"}"#,
            now,
        );
        assert_eq!(
            collider.poll_write(),
            Some(BrowserOutput::Text {
                connection_id: 20,
                text: r#"{"msg":"candidate","error":""}"#.into(),
            })
        );
    }

    #[test]
    fn v2_requires_admission_then_acknowledges_and_relays_p2p_signals() {
        let now = Instant::now();
        let mut collider = Collider::new(Duration::from_secs(10));
        let first_token = match authority(
            &mut collider,
            1,
            AuthorityOperation::AdmitV2 {
                room_id: 42,
                client_id: 101,
                admission_token: "token-101".into(),
                now,
            },
        ) {
            AuthorityResult::AdmittedV2 {
                mode: v2::RoomMode::P2p,
                admission_token,
                is_initiator: Some(true),
                signal_epoch: 0,
            } => admission_token,
            result => panic!("unexpected first admission: {result:?}"),
        };
        let second_token = match authority(
            &mut collider,
            2,
            AuthorityOperation::AdmitV2 {
                room_id: 42,
                client_id: 102,
                admission_token: "token-102".into(),
                now,
            },
        ) {
            AuthorityResult::AdmittedV2 {
                mode: v2::RoomMode::P2p,
                admission_token,
                is_initiator: Some(false),
                signal_epoch: 0,
            } => admission_token,
            result => panic!("unexpected second admission: {result:?}"),
        };

        connect(&mut collider, 10);
        connect(&mut collider, 20);
        text(
            &mut collider,
            10,
            &format!(
                r#"{{"cmd":"register","roomid":"42","clientid":"101","ver":2,"token":"{first_token}"}}"#
            ),
            now,
        );
        assert_eq!(
            collider.poll_write(),
            Some(BrowserOutput::Text {
                connection_id: 10,
                text: r#"{"control":"registered","roomid":"42","epoch":"0","mode":"p2p","is_initiator":true}"#.into(),
            })
        );
        text(
            &mut collider,
            20,
            &format!(
                r#"{{"cmd":"register","roomid":"42","clientid":"102","ver":2,"token":"{second_token}"}}"#
            ),
            now,
        );
        assert_eq!(
            collider.poll_write(),
            Some(BrowserOutput::Text {
                connection_id: 20,
                text: r#"{"control":"registered","roomid":"42","epoch":"0","mode":"p2p","is_initiator":false}"#.into(),
            })
        );

        text(
            &mut collider,
            10,
            r#"{"cmd":"send","epoch":"0","msg":"candidate"}"#,
            now,
        );
        assert_eq!(
            collider.poll_write(),
            Some(BrowserOutput::Text {
                connection_id: 20,
                text: r#"{"msg":"candidate","error":""}"#.into(),
            })
        );
        text(
            &mut collider,
            10,
            r#"{"cmd":"send","epoch":"1","msg":"stale offer"}"#,
            now,
        );
        text(
            &mut collider,
            10,
            r#"{"cmd":"send","msg":"missing epoch"}"#,
            now,
        );
        assert_eq!(collider.poll_write(), None);
    }

    #[test]
    fn v2_rejects_invalid_ids_tokens_and_third_member() {
        let now = Instant::now();
        let mut collider = Collider::new(Duration::from_secs(10));
        connect(&mut collider, 1);
        text(
            &mut collider,
            1,
            r#"{"cmd":"register","roomid":"042","clientid":"1","ver":2,"token":"x"}"#,
            now,
        );
        assert_error_and_close(&mut collider, 1, "INVALID_ROOM_ID");

        connect(&mut collider, 2);
        text(
            &mut collider,
            2,
            r#"{"cmd":"register","roomid":"42","clientid":"1","ver":2,"token":"wrong"}"#,
            now,
        );
        assert_error_and_close(&mut collider, 2, "UNAUTHORIZED");

        for (request_id, client_id) in [(1, 1), (2, 2)] {
            assert!(matches!(
                authority(
                    &mut collider,
                    request_id,
                    AuthorityOperation::AdmitV2 {
                        room_id: 42,
                        client_id,
                        admission_token: format!("token-{client_id}"),
                        now,
                    }
                ),
                AuthorityResult::AdmittedV2 { .. }
            ));
        }
        assert_eq!(
            authority(
                &mut collider,
                3,
                AuthorityOperation::AdmitV2 {
                    room_id: 42,
                    client_id: 3,
                    admission_token: "token-3".into(),
                    now,
                }
            ),
            AuthorityResult::Error {
                result: "NO_SFU_AVAILABLE".into()
            }
        );
    }

    #[test]
    fn v2_leave_closes_member_and_promotes_survivor() {
        let now = Instant::now();
        let mut collider = Collider::new(Duration::from_secs(10));
        let mut tokens = Vec::new();
        for (request_id, client_id) in [(1, 1), (2, 2)] {
            let AuthorityResult::AdmittedV2 {
                admission_token, ..
            } = authority(
                &mut collider,
                request_id,
                AuthorityOperation::AdmitV2 {
                    room_id: 9,
                    client_id,
                    admission_token: format!("token-{client_id}"),
                    now,
                },
            )
            else {
                panic!("admission failed");
            };
            tokens.push(admission_token);
            connect(&mut collider, client_id);
            text(
                &mut collider,
                client_id,
                &format!(
                    r#"{{"cmd":"register","roomid":"9","clientid":"{client_id}","ver":2,"token":"{}"}}"#,
                    tokens.last().unwrap()
                ),
                now,
            );
            let _ = collider.poll_write();
        }

        assert_eq!(
            authority(
                &mut collider,
                3,
                AuthorityOperation::RemoveV2 {
                    room_id: 9,
                    client_id: 2,
                    admission_token: tokens[1].clone(),
                }
            ),
            AuthorityResult::RemovedV2
        );
        assert_eq!(
            collider.poll_write(),
            Some(BrowserOutput::Close { connection_id: 2 })
        );
        assert_eq!(
            collider.poll_write(),
            Some(BrowserOutput::Text {
                connection_id: 1,
                text: r#"{"control":"p2p-promote","roomid":"9","epoch":"0","is_initiator":true}"#
                    .into(),
            })
        );
    }

    #[test]
    fn v2_registered_snapshot_precedes_queued_offer_and_trickle_ice() {
        let now = Instant::now();
        let mut collider = Collider::new(Duration::from_secs(10));
        for (request_id, client_id) in [(1, 1), (2, 2)] {
            assert!(matches!(
                authority(
                    &mut collider,
                    request_id,
                    AuthorityOperation::AdmitV2 {
                        room_id: 5,
                        client_id,
                        admission_token: format!("token-{client_id}"),
                        now,
                    }
                ),
                AuthorityResult::AdmittedV2 { .. }
            ));
        }
        connect(&mut collider, 1);
        text(
            &mut collider,
            1,
            r#"{"cmd":"register","roomid":"5","clientid":"1","ver":2,"token":"token-1"}"#,
            now,
        );
        let _ = collider.poll_write();
        text(
            &mut collider,
            1,
            r#"{"cmd":"send","epoch":"0","msg":"offer"}"#,
            now,
        );
        text(
            &mut collider,
            1,
            r#"{"cmd":"send","epoch":"0","msg":"candidate"}"#,
            now,
        );
        connect(&mut collider, 2);
        text(
            &mut collider,
            2,
            r#"{"cmd":"register","roomid":"5","clientid":"2","ver":2,"token":"token-2"}"#,
            now,
        );
        assert!(matches!(
            collider.poll_write(),
            Some(BrowserOutput::Text { text, .. }) if text.contains("registered")
        ));
        assert_eq!(
            collider.poll_write(),
            Some(BrowserOutput::Text {
                connection_id: 2,
                text: server_msg("offer"),
            })
        );
        assert_eq!(
            collider.poll_write(),
            Some(BrowserOutput::Text {
                connection_id: 2,
                text: server_msg("candidate"),
            })
        );
    }

    #[test]
    fn v2_decimal_ids_are_canonical_u64_strings() {
        assert_eq!(canonical_u64("0"), Some(0));
        assert_eq!(canonical_u64("18446744073709551615"), Some(u64::MAX));
        for invalid in [
            "",
            "00",
            "01",
            "+1",
            "-1",
            " 1",
            "1 ",
            "1.0",
            "18446744073709551616",
        ] {
            assert_eq!(canonical_u64(invalid), None, "accepted {invalid:?}");
        }
    }

    #[test]
    fn v1_protocol_errors_are_framed_then_close_the_socket() {
        let now = Instant::now();
        let mut collider = Collider::new(Duration::from_secs(10));
        connect(&mut collider, 1);
        text(&mut collider, 1, r#"{"cmd":"send","msg":"offer"}"#, now);
        assert_error_and_close(&mut collider, 1, "Client not registered");
    }

    #[test]
    fn all_v1_validation_failures_preserve_legacy_errors() {
        let now = Instant::now();
        let mut collider = Collider::new(Duration::from_secs(10));

        connect(&mut collider, 1);
        text(
            &mut collider,
            1,
            r#"{"cmd":"register","roomid":"","clientid":"client"}"#,
            now,
        );
        assert_error_and_close(
            &mut collider,
            1,
            "Invalid register request: missing 'clientid' or 'roomid'",
        );

        connect(&mut collider, 2);
        text(
            &mut collider,
            2,
            r#"{"cmd":"register","roomid":"opaque-room","clientid":"opaque-client"}"#,
            now,
        );
        assert_eq!(collider.poll_write(), None);
        text(
            &mut collider,
            2,
            r#"{"cmd":"register","roomid":"opaque-room","clientid":"opaque-client"}"#,
            now,
        );
        assert_error_and_close(&mut collider, 2, "Duplicated register request");

        connect(&mut collider, 3);
        text(
            &mut collider,
            3,
            r#"{"cmd":"register","roomid":"other","clientid":"other"}"#,
            now,
        );
        text(&mut collider, 3, r#"{"cmd":"send","msg":""}"#, now);
        assert_error_and_close(&mut collider, 3, "Invalid send request: missing 'msg'");

        connect(&mut collider, 4);
        text(&mut collider, 4, r#"{"cmd":"wat"}"#, now);
        assert_error_and_close(&mut collider, 4, "Invalid message: unexpected 'cmd'");
    }

    #[test]
    fn duplicate_live_client_registration_closes_only_the_new_socket() {
        let now = Instant::now();
        let mut collider = Collider::new(Duration::from_secs(10));
        connect(&mut collider, 1);
        connect(&mut collider, 2);
        for connection_id in [1, 2] {
            text(
                &mut collider,
                connection_id,
                r#"{"cmd":"register","roomid":"room","clientid":"client"}"#,
                now,
            );
        }
        assert_error_and_close(&mut collider, 2, "Duplicated registration");
        assert_eq!(
            collider.connections.get(&("room".into(), "client".into())),
            Some(&1)
        );
    }

    #[test]
    fn authority_remove_closes_a_live_browser_connection() {
        let now = Instant::now();
        let mut collider = Collider::new(Duration::from_secs(10));
        connect(&mut collider, 1);
        text(
            &mut collider,
            1,
            r#"{"cmd":"register","roomid":"room","clientid":"client"}"#,
            now,
        );
        assert_eq!(
            authority(
                &mut collider,
                1,
                AuthorityOperation::Remove {
                    roomid: "room".into(),
                    clientid: "client".into(),
                }
            ),
            AuthorityResult::Removed
        );
        assert_eq!(
            collider.poll_write(),
            Some(BrowserOutput::Close { connection_id: 1 })
        );
    }

    #[test]
    fn authority_admit_includes_queued_messages_for_v1_join() {
        let now = Instant::now();
        let mut collider = Collider::new(Duration::from_secs(10));
        assert!(matches!(
            authority(
                &mut collider,
                1,
                AuthorityOperation::Inject {
                    roomid: "room".into(),
                    clientid: "1".into(),
                    msg: "offer".into(),
                    now,
                }
            ),
            AuthorityResult::Injected
        ));
        assert_eq!(
            authority(
                &mut collider,
                2,
                AuthorityOperation::Admit {
                    roomid: "room".into(),
                    clientid: "2".into(),
                    is_loopback: false,
                    now,
                }
            ),
            AuthorityResult::Admitted {
                is_initiator: false,
                messages: vec!["offer".into()],
            }
        );
    }

    #[test]
    fn stale_disconnect_does_not_deregister_replacement_connection() {
        let now = Instant::now();
        let mut collider = Collider::new(Duration::from_secs(10));
        connect(&mut collider, 1);
        text(
            &mut collider,
            1,
            r#"{"cmd":"register","roomid":"room","clientid":"1"}"#,
            now,
        );
        collider
            .handle_read(BrowserInput::Disconnected {
                connection_id: 1,
                now,
            })
            .unwrap();
        connect(&mut collider, 2);
        text(
            &mut collider,
            2,
            r#"{"cmd":"register","roomid":"room","clientid":"1"}"#,
            now,
        );
        collider
            .handle_read(BrowserInput::Disconnected {
                connection_id: 1,
                now,
            })
            .unwrap();
        assert_eq!(
            collider.connections.get(&("room".into(), "1".into())),
            Some(&2)
        );
    }
}
