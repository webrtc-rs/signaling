use crate::client::ClientId;
use crate::messages::Message;
use crate::room::{Room, RoomId};
use sansio::Protocol;
use std::collections::HashMap;
use std::convert::Infallible;
use std::time::{Duration, Instant};

/// A map of signaling rooms.
///
/// The Go implementation protects this map with a mutex. The signaling crate is
/// Sans-IO and is driven through exclusive `&mut self` access, so synchronization
/// belongs to the caller and no lock is needed here.
pub struct RoomTable {
    rooms: HashMap<RoomId, Room>,
    register_timeout: Duration,
}

impl RoomTable {
    pub fn new(register_timeout: Duration) -> Self {
        Self {
            rooms: HashMap::new(),
            register_timeout,
        }
    }

    /// Return the room specified by `id`, creating it when it does not exist.
    pub fn room(&mut self, id: &RoomId) -> &mut Room {
        self.rooms
            .entry(id.clone())
            .or_insert_with(|| Room::new(id.clone(), self.register_timeout))
    }

    /// Remove a client and remove its room as well when it becomes empty.
    pub fn remove(&mut self, room_id: &RoomId, client_id: &ClientId) {
        let remove_room = if let Some(room) = self.rooms.get_mut(room_id) {
            room.remove(client_id);
            room.empty()
        } else {
            false
        };

        if remove_room {
            self.rooms.remove(room_id);
        }
    }

    /// Forward a message to a room, creating that room when necessary.
    pub fn send(
        &mut self,
        now: Instant,
        room_id: &RoomId,
        source_id: &ClientId,
        msg: String,
    ) -> Result<(), String> {
        self.room(room_id).send(now, source_id, msg)
    }

    /// Store or relay an outbound client message.
    ///
    /// This is the in-process replacement for the old room-server-to-Collider
    /// HTTP bridge. `Room::send` queues when the peer is absent and relays when
    /// the peer is present.
    pub fn save_or_send(
        &mut self,
        now: Instant,
        room_id: &RoomId,
        source_id: &ClientId,
        msg: String,
    ) -> Result<(), String> {
        self.send(now, room_id, source_id, msg)
    }

    /// Allocate a client for a `/join` request and return its initiator flag plus
    /// messages queued by the other client.
    pub fn join(
        &mut self,
        now: Instant,
        room_id: &RoomId,
        client_id: &ClientId,
        is_loopback: bool,
    ) -> Result<(bool, Vec<String>), String> {
        self.room(room_id).add_client(now, client_id, is_loopback)
    }

    /// Remove a client for a browser `/leave` or an internal removal.
    pub fn leave(&mut self, room_id: &RoomId, client_id: &ClientId) {
        self.remove(room_id, client_id);
    }

    /// Return the number of clients in a room, or zero when the room is absent.
    pub fn occupancy(&self, room_id: &RoomId) -> usize {
        self.rooms.get(room_id).map_or(0, Room::occupancy)
    }

    /// Register a client's connection, creating the room or client if needed.
    pub fn register(
        &mut self,
        now: Instant,
        room_id: &RoomId,
        client_id: &ClientId,
    ) -> Result<(), String> {
        self.room(room_id).register(now, client_id)
    }

    /// Clear a client's connection registration and arm its reconnect timeout.
    /// The client remains in the room so it can reconnect while roaming networks.
    pub fn deregister(&mut self, now: Instant, room_id: &RoomId, client_id: &ClientId) {
        if let Some(room) = self.rooms.get_mut(room_id) {
            room.deregister(now, client_id);
        }
    }

    /// Remove a client only if it is still unregistered, then reap an empty room.
    ///
    /// In the Sans-IO implementation the client's deadline identifies the client
    /// incarnation: registration cancels that deadline, so a stale timeout cannot
    /// remove a client that has re-registered.
    pub fn remove_if_unregistered(&mut self, room_id: &RoomId, client_id: &ClientId) {
        let remove_room = self
            .rooms
            .get_mut(room_id)
            .is_some_and(|room| room.remove_if_unregistered(client_id) && room.empty());

        if remove_room {
            self.rooms.remove(room_id);
        }
    }

    /// Return the number of currently registered client connections.
    pub fn ws_count(&self) -> usize {
        self.rooms.values().map(Room::ws_count).sum()
    }

    pub fn room_count(&self) -> usize {
        self.rooms.len()
    }

    pub fn client_count(&self) -> usize {
        self.rooms.values().map(Room::occupancy).sum()
    }
}

impl Protocol<Message, Message, Infallible> for RoomTable {
    /// A client message surfaced by a room, tagged with its room and client IDs.
    type Rout = Message;
    /// A server message ready for a client, tagged with its room and client IDs.
    type Wout = Message;
    type Eout = Infallible;
    type Error = String;
    type Time = Instant;

    /// Deliver a message read from a client's connection to its room. Messages for
    /// rooms that have already been removed are dropped.
    fn handle_read(&mut self, msg: Message) -> Result<(), Self::Error> {
        if let Some(room) = self.rooms.get_mut(&msg.roomid) {
            room.handle_read(msg)?;
        }
        Ok(())
    }

    /// Drain the next client message from any room and attach its room ID.
    fn poll_read(&mut self) -> Option<Self::Rout> {
        for room in self.rooms.values_mut() {
            if let Some(msg) = room.poll_read() {
                return Some(msg);
            }
        }
        None
    }

    /// Deliver an outbound server message to its room and client.
    /// Messages for rooms that have already been removed are dropped.
    fn handle_write(&mut self, msg: Message) -> Result<(), Self::Error> {
        if let Some(room) = self.rooms.get_mut(&msg.roomid) {
            room.handle_write(msg)?;
        }
        Ok(())
    }

    /// Drain the next client-bound message from any room and attach its room ID.
    fn poll_write(&mut self) -> Option<Self::Wout> {
        for room in self.rooms.values_mut() {
            if let Some(msg) = room.poll_write() {
                return Some(msg);
            }
        }
        None
    }

    fn handle_event(&mut self, evt: Infallible) -> Result<(), Self::Error> {
        match evt {}
    }

    /// Drive every room's client registration deadlines and reap rooms whose last
    /// client expired.
    fn handle_timeout(&mut self, now: Self::Time) -> Result<(), Self::Error> {
        for room in self.rooms.values_mut() {
            room.handle_timeout(now)?;
        }
        self.rooms.retain(|_, room| !room.empty());
        Ok(())
    }

    /// Return the earliest client registration deadline across all rooms.
    fn poll_timeout(&mut self) -> Option<Self::Time> {
        self.rooms.values_mut().filter_map(Room::poll_timeout).min()
    }

    fn close(&mut self) -> Result<(), Self::Error> {
        for room in self.rooms.values_mut() {
            room.close()?;
        }
        self.rooms.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids() -> (RoomId, ClientId, ClientId) {
        ("room".to_string(), "1".to_string(), "2".to_string())
    }

    fn message(room_id: &str, client_id: &str, msg: &str) -> Message {
        Message {
            roomid: room_id.to_string(),
            clientid: client_id.to_string(),
            msg: msg.to_string(),
        }
    }

    fn assert_message(actual: Option<Message>, room_id: &str, client_id: &str, msg: &str) {
        let actual = actual.expect("expected a message");
        assert_eq!(actual.roomid, room_id);
        assert_eq!(actual.clientid, client_id);
        assert_eq!(actual.msg, msg);
    }

    #[test]
    fn room_gets_or_creates() {
        let mut table = RoomTable::new(Duration::from_secs(10));
        let room_id = "room".to_string();

        assert_eq!(table.room(&room_id).id(), &room_id);
        assert_eq!(table.room_count(), 1);
        assert_eq!(table.room(&room_id).id(), &room_id);
        assert_eq!(table.room_count(), 1);
    }

    #[test]
    fn remove_reaps_an_empty_room_and_promotes_a_survivor() {
        let now = Instant::now();
        let mut table = RoomTable::new(Duration::from_secs(10));
        let (room_id, client1, client2) = ids();

        assert!(table.join(now, &room_id, &client1, false).unwrap().0);
        assert!(!table.join(now, &room_id, &client2, false).unwrap().0);
        table.remove(&room_id, &client1);

        assert_eq!(table.occupancy(&room_id), 1);
        assert!(table.room(&room_id).client_is_initiator(&client2));

        table.remove(&room_id, &client2);
        assert_eq!(table.occupancy(&room_id), 0);
        assert_eq!(table.room_count(), 0);
    }

    #[test]
    fn send_and_save_or_send_create_rooms_and_queue_messages() {
        let now = Instant::now();
        let mut table = RoomTable::new(Duration::from_secs(10));
        let (room_id, client1, _) = ids();

        table
            .send(now, &room_id, &client1, "offer".to_string())
            .unwrap();
        table
            .save_or_send(now, &room_id, &client1, "candidate".to_string())
            .unwrap();

        let (_, messages) = table
            .join(now, &room_id, &"new-client".to_string(), false)
            .unwrap();
        assert_eq!(messages, ["offer", "candidate"]);
    }

    #[test]
    fn join_leave_and_occupancy_match_room_server_operations() {
        let now = Instant::now();
        let mut table = RoomTable::new(Duration::from_secs(10));
        let (room_id, client1, client2) = ids();

        let (initiator, messages) = table.join(now, &room_id, &client1, false).unwrap();
        assert!(initiator);
        assert!(messages.is_empty());
        assert_eq!(table.occupancy(&room_id), 1);

        assert!(!table.join(now, &room_id, &client2, false).unwrap().0);
        assert_eq!(table.occupancy(&room_id), 2);

        table.leave(&room_id, &client1);
        table.leave(&room_id, &client2);
        assert_eq!(table.occupancy(&room_id), 0);
    }

    #[test]
    fn register_deregister_and_remove_if_unregistered() {
        let now = Instant::now();
        let timeout = Duration::from_secs(10);
        let mut table = RoomTable::new(timeout);
        let (room_id, client1, _) = ids();

        table.register(now, &room_id, &client1).unwrap();
        assert_eq!(table.ws_count(), 1);

        table.deregister(now, &room_id, &client1);
        assert_eq!(table.ws_count(), 0);
        assert_eq!(table.occupancy(&room_id), 1);

        table.remove_if_unregistered(&room_id, &client1);
        assert_eq!(table.room_count(), 0);
    }

    #[test]
    fn remove_if_unregistered_preserves_registered_client() {
        let now = Instant::now();
        let mut table = RoomTable::new(Duration::from_secs(10));
        let (room_id, client1, _) = ids();

        table.register(now, &room_id, &client1).unwrap();
        table.remove_if_unregistered(&room_id, &client1);

        assert_eq!(table.occupancy(&room_id), 1);
        assert_eq!(table.ws_count(), 1);
    }

    #[test]
    fn protocol_read_plane_preserves_room_and_client_address() {
        let now = Instant::now();
        let mut table = RoomTable::new(Duration::from_secs(10));
        let (room_id, client1, _) = ids();
        table.join(now, &room_id, &client1, false).unwrap();

        table
            .handle_read(message(&room_id, &client1, "offer"))
            .unwrap();

        assert_message(table.poll_read(), &room_id, &client1, "offer");
        assert!(table.poll_read().is_none());
    }

    #[test]
    fn protocol_write_plane_preserves_room_and_client_address() {
        let now = Instant::now();
        let mut table = RoomTable::new(Duration::from_secs(10));
        let (room_id, client1, _) = ids();
        table.register(now, &room_id, &client1).unwrap();

        table
            .handle_write(message(&room_id, &client1, "answer"))
            .unwrap();

        assert_message(table.poll_write(), &room_id, &client1, "answer");
        assert!(table.poll_write().is_none());
    }

    #[test]
    fn protocol_drops_messages_for_unknown_rooms() {
        let mut table = RoomTable::new(Duration::from_secs(10));
        let (room_id, client1, _) = ids();

        table
            .handle_read(message(&room_id, &client1, "read"))
            .unwrap();
        table
            .handle_write(message(&room_id, &client1, "write"))
            .unwrap();

        assert_eq!(table.room_count(), 0);
        assert!(table.poll_read().is_none());
        assert!(table.poll_write().is_none());
    }

    #[test]
    fn protocol_timeout_reaps_expired_clients_and_empty_rooms() {
        let now = Instant::now();
        let timeout = Duration::from_secs(10);
        let mut table = RoomTable::new(timeout);
        let (room_id, client1, _) = ids();
        table.join(now, &room_id, &client1, false).unwrap();

        assert_eq!(table.poll_timeout(), Some(now + timeout));
        table.handle_timeout(now + timeout).unwrap();

        assert_eq!(table.room_count(), 0);
        assert!(table.poll_timeout().is_none());
    }

    #[test]
    fn protocol_timeout_uses_the_earliest_room_deadline() {
        let now = Instant::now();
        let timeout = Duration::from_secs(10);
        let mut table = RoomTable::new(timeout);
        let client = "client".to_string();
        let room1 = "room-1".to_string();
        let room2 = "room-2".to_string();

        table.join(now, &room1, &client, false).unwrap();
        table
            .join(now + Duration::from_secs(2), &room2, &client, false)
            .unwrap();

        assert_eq!(table.poll_timeout(), Some(now + timeout));
    }

    #[test]
    fn protocol_close_clears_all_rooms() {
        let now = Instant::now();
        let mut table = RoomTable::new(Duration::from_secs(10));
        let (room_id, client1, _) = ids();
        table.register(now, &room_id, &client1).unwrap();

        table.close().unwrap();

        assert_eq!(table.room_count(), 0);
        assert_eq!(table.ws_count(), 0);
    }
}
