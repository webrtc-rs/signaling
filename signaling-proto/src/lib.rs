#![warn(rust_2018_idioms)]

pub mod v2 {
    include!(concat!(env!("OUT_DIR"), "/signaling.v2.rs"));
}

#[cfg(test)]
mod tests {
    use super::v2::{AdmitV1Request, AppId, RequestContext};
    use prost::Message;

    #[test]
    fn v2_apprtc_request_round_trips_as_protobuf() {
        let request = AdmitV1Request {
            context: Some(RequestContext {
                app_id: AppId::Apprtc as i32,
                instance_id: "apprtc-test".into(),
                request_id: 7,
            }),
            room_id: "opaque-room".into(),
            client_id: "opaque-client".into(),
            is_loopback: true,
        };
        assert_eq!(
            AdmitV1Request::decode(request.encode_to_vec().as_slice()).unwrap(),
            request
        );
    }
}
