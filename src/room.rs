use crate::client::{Client, ClientEvent, ClientId};
use crate::messages::Message;
use sansio::Protocol;
use std::collections::HashMap;
use std::convert::Infallible;
use std::time::{Duration, Instant};

const MAX_ROOM_CAPACITY: usize = 2;

/// The synthetic peer added in loopback debug mode (was `constants.LOOPBACK_CLIENT_ID`).
/// This is a room concept — only `add_client`'s loopback branch uses it — so it lives
/// here rather than crossing the `signaling`/`apprtc` crate boundary.
const LOOPBACK_CLIENT_ID: &str = "LOOPBACK_CLIENT_ID";

pub type RoomId = String;

/// A room and its (at most two) clients.
///
/// The Go `parent *roomTable` back-pointer is gone: it existed only to arm a
/// `time.AfterFunc` that reached back up to `removeIfUnregistered`. Here the room
/// implements [`sansio::Protocol`] and reaps unregistered clients from
/// [`Protocol::handle_timeout`] instead (each client surfaces
/// [`ClientEvent::Expired`] when its register-timeout elapses).
pub struct Room {
    id: RoomId,
    /// Client id -> client.
    clients: HashMap<ClientId, Client>,
    register_timeout: Duration,
}

impl Room {
    pub fn new(id: RoomId, register_timeout: Duration) -> Self {
        Self {
            id,
            clients: HashMap::new(),
            register_timeout,
        }
    }

    pub fn id(&self) -> &RoomId {
        &self.id
    }

    /// Return the client, creating it (with a fresh register-timeout deadline) if it
    /// does not exist and the room is not full. In Go this armed the `AfterFunc`
    /// timer; here it just seeds the client's deadline, which the room polls.
    fn client(&mut self, now: Instant, client_id: &ClientId) -> Result<&mut Client, String> {
        if !self.clients.contains_key(client_id) {
            if self.clients.len() >= MAX_ROOM_CAPACITY {
                return Err("Max room capacity reached".to_string());
            }
            let timeout = Some(now + self.register_timeout);
            self.clients.insert(
                client_id.clone(),
                Client::new(client_id.clone(), self.id.clone(), timeout),
            );
        }
        Ok(self
            .clients
            .get_mut(client_id)
            .expect("client just inserted or already present"))
    }

    /// Register a client's connection, then flush the other client's queued messages
    /// to it.
    pub fn register(&mut self, now: Instant, client_id: &ClientId) -> Result<(), String> {
        self.client(now, client_id)?.register()?;

        // Send the queued messages from the other client of the room.
        if self.clients.len() > 1
            && let Some(other_id) = self
                .clients
                .keys()
                .find(|k| k.as_str() != client_id.as_str())
                .cloned()
            && let [Some(other), Some(c)] = self.clients.get_disjoint_mut([&other_id, client_id])
        {
            let _ = other.send_queued(c);
        }
        Ok(())
    }

    /// Send `msg` to the other client of the room, or queue it on the source client if
    /// the other client has not joined.
    pub fn send(&mut self, now: Instant, src_id: &ClientId, msg: String) -> Result<(), String> {
        self.client(now, src_id)?;

        // Queue the message if the other client has not joined.
        if self.clients.len() == 1 {
            return self.clients.get_mut(src_id).unwrap().enqueue(msg);
        }

        // Send the message to the other client of the room.
        if let Some(other_id) = self
            .clients
            .keys()
            .find(|k| k.as_str() != src_id.as_str())
            .cloned()
            && let [Some(src), Some(oc)] = self.clients.get_disjoint_mut([src_id, &other_id])
        {
            return src.send(oc, msg);
        }

        // The room must be corrupted.
        Err(format!("Corrupted room {}", self.id))
    }

    /// Remove the client and promote the surviving client (if any) to initiator so it
    /// can accept a new peer. The hub closes the removed client's socket (the Go
    /// `deregister()` → `rwc.Close()`); dropping the [`Client`] is enough here.
    pub fn remove(&mut self, client_id: &ClientId) {
        if let Some(mut client) = self.clients.remove(client_id) {
            client.deregister();
            for other in self.clients.values_mut() {
                other.set_initiator(true);
            }
        }
    }

    /// Clear a client's live registration and arm the reconnect grace period.
    pub fn deregister(&mut self, now: Instant, client_id: &ClientId) -> bool {
        if let Some(client) = self.clients.get_mut(client_id)
            && client.registered()
        {
            client.deregister();
            client.set_timer(Some(now + self.register_timeout));
            return true;
        }
        false
    }

    /// Remove a client only when it is still unregistered.
    pub fn remove_if_unregistered(&mut self, client_id: &ClientId) -> bool {
        if self
            .clients
            .get(client_id)
            .is_some_and(|client| !client.registered())
        {
            self.remove(client_id);
            return true;
        }
        false
    }

    pub fn client_is_initiator(&self, client_id: &ClientId) -> bool {
        self.clients
            .get(client_id)
            .is_some_and(Client::is_initiator)
    }

    /// Allocate a new client for a `/join` request: elect the initiator (first client
    /// in the room) and return the messages queued by the other client so the joiner
    /// can replay the existing offer/ICE. Port of `apprtc.py::add_client_to_room`.
    pub fn add_client(
        &mut self,
        now: Instant,
        client_id: &ClientId,
        is_loopback: bool,
    ) -> Result<(bool, Vec<String>), String> {
        if self.clients.contains_key(client_id) {
            return Err("DUPLICATE_CLIENT".to_string());
        }
        if self.clients.len() >= MAX_ROOM_CAPACITY {
            return Err("FULL".to_string());
        }

        let is_initiator = self.clients.is_empty();

        let mut messages = Vec::new();
        if !is_initiator {
            // Hand the joiner the initiator's queued offer/ICE and clear the queue.
            for other in self.clients.values_mut() {
                messages.extend(other.drain_msgs());
            }
        }

        self.client(now, client_id)?.set_initiator(is_initiator);

        if is_loopback {
            // Mirror the loopback debug path: add a second, non-initiator client.
            if let Ok(lc) = self.client(now, &LOOPBACK_CLIENT_ID.to_string()) {
                lc.set_initiator(false);
            }
        }
        Ok((is_initiator, messages))
    }

    /// The number of clients in the room.
    pub fn occupancy(&self) -> usize {
        self.clients.len()
    }

    /// True if there is no client in the room.
    pub fn empty(&self) -> bool {
        self.clients.is_empty()
    }

    /// The number of clients with a live connection registered.
    pub fn ws_count(&self) -> usize {
        self.clients.values().filter(|c| c.registered()).count()
    }
}

impl Protocol<Message, Message, Infallible> for Room {
    /// A message received on a client's socket, tagged with its client id.
    type Rout = Message;
    /// A message to write to a client's socket, tagged with its client id.
    type Wout = Message;
    type Eout = Infallible;
    type Error = String;
    type Time = Instant;

    /// Route an inbound message to the client. An unknown client is dropped
    fn handle_read(&mut self, msg: Message) -> Result<(), Self::Error> {
        if let Some(c) = self.clients.get_mut(&msg.clientid) {
            c.handle_read(msg)?;
        }
        Ok(())
    }

    /// Drain the next message received from any client, tagged with its id.
    fn poll_read(&mut self) -> Option<Self::Rout> {
        for client in self.clients.values_mut() {
            if let Some(msg) = client.poll_read() {
                return Some(msg);
            }
        }
        None
    }

    /// Deliver a message to the client (dropped if the client is unknown).
    fn handle_write(&mut self, msg: Message) -> Result<(), Self::Error> {
        if let Some(c) = self.clients.get_mut(&msg.clientid) {
            c.handle_write(msg)?;
        }
        Ok(())
    }

    /// Drain the next server->client message from any client, tagged with its id, for
    /// the driver to frame and write to that client's socket.
    fn poll_write(&mut self) -> Option<Self::Wout> {
        for client in self.clients.values_mut() {
            if let Some(msg) = client.poll_write() {
                return Some(msg);
            }
        }
        None
    }

    /// Tick every client and reap those whose register-timeout elapsed while still
    /// unregistered. This is the sans-IO `removeIfUnregistered`: a client surfaces
    /// [`ClientEvent::Expired`], and the room removes it (promoting any survivor).
    fn handle_timeout(&mut self, now: Self::Time) -> Result<(), Self::Error> {
        let mut expired: Vec<ClientId> = Vec::new();
        for (id, c) in self.clients.iter_mut() {
            c.handle_timeout(now)?;
            while let Some(evt) = c.poll_event() {
                match evt {
                    ClientEvent::Expired => expired.push(id.clone()),
                }
            }
        }
        for id in &expired {
            self.remove(id);
        }
        Ok(())
    }

    /// The earliest register-timeout deadline across the room's clients.
    fn poll_timeout(&mut self) -> Option<Self::Time> {
        let mut eto: Option<Instant> = None;
        for c in self.clients.values_mut() {
            if let Some(next) = c.poll_timeout() {
                eto = Some(eto.map_or(next, |cur| cur.min(next)));
            }
        }
        eto
    }

    fn close(&mut self) -> Result<(), Self::Error> {
        for c in self.clients.values_mut() {
            c.close()?;
        }
        self.clients.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sansio::Protocol;

    fn new_room(id: &str) -> Room {
        Room::new(id.to_string(), Duration::from_secs(1))
    }

    fn assert_message(actual: Option<Message>, room_id: &str, client_id: &str, msg: &str) {
        let actual = actual.expect("expected a message");
        assert_eq!(actual.roomid, room_id);
        assert_eq!(actual.clientid, client_id);
        assert_eq!(actual.msg, msg);
    }

    #[test]
    fn new_room_is_empty() {
        let r = new_room("abc");
        assert_eq!(r.id(), "abc");
        assert_eq!(r.occupancy(), 0);
    }

    #[test]
    fn get_or_create_client() {
        let now = Instant::now();
        let mut r = new_room("ab");
        let (id1, id2, id3) = ("1".to_string(), "2".to_string(), "3".to_string());

        // Create, then mutate and re-read to prove the second call returns the same
        // client rather than recreating it (Go compared pointers).
        r.client(now, &id1)
            .expect("first client")
            .set_initiator(true);
        assert!(r.client(now, &id1).expect("same client").is_initiator());
        assert_eq!(
            r.occupancy(),
            1,
            "client() must not recreate an existing client"
        );

        r.client(now, &id2).expect("second client");
        assert_eq!(r.occupancy(), 2);

        // The third client exceeds capacity.
        assert!(r.client(now, &id3).is_err());
    }

    #[test]
    fn register_delivers_queued_message() {
        let now = Instant::now();
        let mut r = new_room("a");
        let (id1, id2) = ("1".to_string(), "2".to_string());

        r.client(now, &id1)
            .unwrap()
            .enqueue("hello".to_string())
            .unwrap();
        r.register(now, &id2).unwrap();

        assert_eq!(r.occupancy(), 2);
        assert!(r.client(now, &id2).unwrap().registered());
        // The first client's queued message is flushed to the newly-registered one
        // (was delivered to `c2.rwc.Msg`).
        assert_message(r.poll_write(), "a", "2", "hello");
    }

    #[test]
    fn send_queues_when_alone() {
        let now = Instant::now();
        let mut r = new_room("a");
        let id = "1".to_string();

        r.send(now, &id, "hi".to_string()).unwrap();
        // Only one client: the message is queued on it, not delivered.
        assert!(r.poll_write().is_none());
        assert_eq!(
            r.client(now, &id).unwrap().drain_msgs(),
            vec!["hi".to_string()]
        );
    }

    #[test]
    fn send_delivers_when_paired() {
        let now = Instant::now();
        let mut r = new_room("a");
        let (id1, id2) = ("1".to_string(), "2".to_string());

        r.register(now, &id2).unwrap();
        r.send(now, &id1, "hi".to_string()).unwrap();

        // Delivered to the peer, not queued on the sender.
        assert!(r.client(now, &id1).unwrap().drain_msgs().is_empty());
        assert_message(r.poll_write(), "a", "2", "hi");
    }

    #[test]
    fn remove_deletes_client() {
        let now = Instant::now();
        let mut r = new_room("a");
        let id = "1".to_string();

        r.register(now, &id).unwrap();
        r.remove(&id);
        // Go asserted `rwc.Closed`; the socket close is driver-side here, so the
        // observable is that the client is gone.
        assert!(r.empty());
    }
}
