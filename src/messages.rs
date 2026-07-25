use crate::client::ClientId;
use crate::room::RoomId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default)]
pub struct Message {
    pub roomid: RoomId,
    pub clientid: ClientId,
    pub msg: String,
}

/// A WebSocket message from the client.
///
/// Fields absent from the frame default to `""` (via `#[serde(default)]`),
/// matching Go's `json.Unmarshal` zero-value behavior — a `send` frame carries
/// only `cmd` and `msg`, a `register` frame only `cmd`/`roomid`/`clientid`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct WsClientMsg {
    #[serde(default)]
    pub cmd: String,
    #[serde(default)]
    pub roomid: String,
    #[serde(default)]
    pub clientid: String,
    #[serde(default)]
    pub msg: String,
    #[serde(default)]
    pub ver: Option<u8>,
    #[serde(default)]
    pub token: String,
    #[serde(default)]
    pub epoch: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct V2Registered {
    pub control: &'static str,
    pub roomid: String,
    pub epoch: String,
    pub mode: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_initiator: Option<bool>,
}

#[derive(Debug, Clone, Serialize)]
pub struct V2Promoted {
    pub control: &'static str,
    pub roomid: String,
    pub epoch: String,
    pub is_initiator: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct V2Upgrade {
    pub control: &'static str,
    pub roomid: String,
    pub epoch: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct V2Downgrade {
    pub control: &'static str,
    pub roomid: String,
    pub epoch: String,
    pub is_initiator: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct V2RoomFailed {
    pub control: &'static str,
    pub roomid: String,
    pub reason: String,
}

/// A message sent to a client on behalf of another client. Both fields are
/// always serialized (no `omitempty`, as in Go): a relay carries `error: ""`,
/// an error carries `msg: ""`.
#[derive(Debug, Clone, Default, Serialize)]
pub struct WsServerMsg {
    pub msg: String,
    pub error: String,
}

impl WsServerMsg {
    /// A server frame carrying the relay payload `msg` (was `sendServerMsg`).
    pub fn msg(msg: impl Into<String>) -> Self {
        Self {
            msg: msg.into(),
            error: String::new(),
        }
    }

    /// A server frame carrying `error` (was `sendServerErr`).
    pub fn err(error: impl Into<String>) -> Self {
        Self {
            msg: String::new(),
            error: error.into(),
        }
    }
}

/// Frame a relay payload as the `{ "msg": … }` wire string the driver writes to
/// a client socket. Port of `sendServerMsg`, minus the write: sans-IO returns the
/// bytes instead of pushing them into an `io.Writer`. (Go's `json.Encoder.Encode`
/// appended a trailing newline; a discrete WebSocket text frame does not need it.)
pub fn server_msg(msg: &str) -> String {
    to_wire(&WsServerMsg::msg(msg))
}

/// Frame an error as the `{ "error": … }` wire string. Port of `sendServerErr`.
pub fn server_err(error: &str) -> String {
    to_wire(&WsServerMsg::err(error))
}

/// Serialize any value to its JSON wire string (was the generic `send`).
/// Serialization of these fixed string structs is infallible, so an encoding
/// error yields an empty string rather than propagating.
pub(crate) fn to_wire<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value).unwrap_or_default()
}
