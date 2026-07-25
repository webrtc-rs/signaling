use crate::messages::Message;
use crate::room::RoomId;
use sansio::Protocol;
use std::collections::VecDeque;
use std::convert::Infallible;
use std::time::Instant;

const MAX_QUEUED_MSG_COUNT: usize = 1024;

pub type ClientId = String;

/// Events a client surfaces up to its room.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientEvent {
    /// The register-timeout elapsed while the client was still unregistered; the
    /// room should reap it. Replaces the Go `time.AfterFunc(… removeIfUnregistered)`
    /// callback, which needed a `*roomTable` back-pointer.
    Expired,
}

/// One participant of a room.
///
/// Implements [`sansio::Protocol`]: the read plane carries a message received on
/// this client's socket up to the room (to route to the peer); the write plane
/// drains messages the room delivered here back down to the socket; the timeout
/// plane drives the register-timeout. The Go `io.ReadWriteCloser` is gone — the
/// hub owns the real socket — and the `time.Timer` became a caller-polled deadline.
pub struct Client {
    id: ClientId,
    room_id: RoomId,
    /// Whether a connection is currently registered (was `rwc != nil`).
    registered: bool,
    /// Messages this client sent that are queued until the peer registers
    /// (`c.msgs` in Go).
    msgs: VecDeque<String>,
    /// Messages received on this client's socket, awaiting `poll_read` by the room.
    reads: VecDeque<Message>,
    /// Server->client messages ready for the driver to write to this client's
    /// socket (replaces the direct `sendServerMsg(rwc, …)` writes). Each entry is
    /// a raw relay payload, framed as a `{ "msg": … }` server frame on the wire.
    writes: VecDeque<Message>,
    /// Events awaiting `poll_event` by the room (currently just [`ClientEvent::Expired`]).
    events: VecDeque<ClientEvent>,
    /// The register-timeout deadline: the hub reaps the client if it is still
    /// unregistered at this instant (was `timer *time.Timer`). `None` once
    /// registered or when no timeout is pending.
    timeout: Option<Instant>,
    /// True for the first client to join a room (the WebRTC caller). Folded in
    /// from `apprtc.py` `Client.is_initiator`.
    is_initiator: bool,
}

impl Client {
    /// `timeout` is the register-timeout deadline (was the `*time.Timer` argument).
    pub fn new(id: ClientId, room_id: RoomId, timeout: Option<Instant>) -> Self {
        Self {
            id,
            room_id,
            registered: false,
            msgs: VecDeque::new(),
            reads: VecDeque::new(),
            writes: VecDeque::new(),
            events: VecDeque::new(),
            timeout,
            is_initiator: false,
        }
    }

    pub fn id(&self) -> &ClientId {
        &self.id
    }

    pub fn room_id(&self) -> &RoomId {
        &self.room_id
    }

    pub fn is_initiator(&self) -> bool {
        self.is_initiator
    }

    pub fn set_initiator(&mut self, is_initiator: bool) {
        self.is_initiator = is_initiator;
    }

    /// Replace the register-timeout deadline. The previous one is simply dropped
    /// (there is no live `time.Timer` to `Stop` in sans-IO).
    pub fn set_timer(&mut self, timeout: Option<Instant>) {
        self.timeout = timeout;
    }

    /// Bind a connection to the client if one is not already registered.
    pub fn register(&mut self) -> Result<(), String> {
        if self.registered {
            return Err("Duplicated registration".to_string());
        }
        self.set_timer(None);
        self.registered = true;
        Ok(())
    }

    /// Drop the client's connection registration. The hub closes the underlying
    /// socket (the Go `rwc.Close()`); the queued `msgs` are kept so a client
    /// roaming between networks can reconnect.
    pub fn deregister(&mut self) {
        self.registered = false;
    }

    pub fn registered(&self) -> bool {
        self.registered
    }

    /// Add a message to this client's pending-flush queue.
    pub fn enqueue(&mut self, msg: String) -> Result<(), String> {
        if self.msgs.len() >= MAX_QUEUED_MSG_COUNT {
            return Err("Too many messages queued for the client".to_string());
        }
        self.msgs.push_back(msg);
        Ok(())
    }

    /// Drain and return this client's pending-flush queue (was `other.msgs; other.msgs = nil`).
    /// Used by `Room::add_client` to hand a joiner the peer's queued offer/ICE.
    pub fn drain_msgs(&mut self) -> Vec<String> {
        self.msgs.drain(..).collect()
    }

    /// Flush this client's queued messages to `other` (which must be registered).
    pub fn send_queued(&mut self, other: &mut Client) -> Result<(), String> {
        if self.id == other.id || !other.registered {
            return Err("Invalid client".to_string());
        }
        for msg in self.msgs.drain(..) {
            other.handle_write(Message {
                roomid: self.room_id.clone(),
                clientid: other.id.clone(),
                msg,
            })?;
        }
        Ok(())
    }

    /// Send `msg` to `other` if it has registered, otherwise queue it on this
    /// client until `other` registers.
    pub fn send(&mut self, other: &mut Client, msg: String) -> Result<(), String> {
        if self.id == other.id {
            return Err("Invalid client".to_string());
        }
        if other.registered {
            other.handle_write(Message {
                roomid: self.room_id.clone(),
                clientid: other.id.clone(),
                msg,
            })?;
            Ok(())
        } else {
            self.enqueue(msg)
        }
    }
}

impl Protocol<Message, Message, Infallible> for Client {
    /// A message received on this client's socket, surfaced up for the room to
    /// route to the peer.
    type Rout = Message;
    /// A message ready to write to this client's socket.
    type Wout = Message;
    type Eout = ClientEvent;
    type Error = String;
    type Time = Instant;

    /// Push a message received on this client's socket.
    fn handle_read(&mut self, msg: Message) -> Result<(), Self::Error> {
        self.reads.push_back(msg);
        Ok(())
    }

    /// Drain the next received message for the room to route.
    fn poll_read(&mut self) -> Option<Self::Rout> {
        self.reads.pop_front()
    }

    /// Deliver a message to this client (the room routing a peer's message here).
    fn handle_write(&mut self, msg: Message) -> Result<(), Self::Error> {
        self.writes.push_back(msg);
        Ok(())
    }

    /// Drain the next server->client message. The hub frames it as a
    /// `{ "msg": … }` server frame and writes it to this client's socket.
    fn poll_write(&mut self) -> Option<Self::Wout> {
        self.writes.pop_front()
    }

    fn poll_event(&mut self) -> Option<Self::Eout> {
        self.events.pop_front()
    }

    /// Emit [`ClientEvent::Expired`] once the register-timeout deadline passes
    /// while the client is still unregistered (the Go `AfterFunc` callback).
    fn handle_timeout(&mut self, now: Self::Time) -> Result<(), Self::Error> {
        if let Some(deadline) = self.timeout
            && now >= deadline
            && !self.registered
        {
            self.events.push_back(ClientEvent::Expired);
            self.timeout = None;
        }
        Ok(())
    }

    /// The pending register-timeout deadline, if any.
    fn poll_timeout(&mut self) -> Option<Self::Time> {
        self.timeout
    }

    /// Release the client's connection registration.
    fn close(&mut self) -> Result<(), Self::Error> {
        self.deregister();
        Ok(())
    }
}
