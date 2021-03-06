use crate::kafka_protocol::protocol_serializable::*;

/// Version 1
///
pub struct FindCoordinatorResponse {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub error_message: Option<String>,
    pub coordinator: Coordinator,
}

#[derive(Clone, Debug)]
pub struct Coordinator {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
}

impl ProtocolDeserializable<FindCoordinatorResponse> for Vec<u8> {
    fn into_protocol_type(self) -> ProtocolDeserializeResult<FindCoordinatorResponse> {
        de_i32(&self[0..=3]).and_then(|throttle_time_ms| {
            de_i16(&self[4..=5]).and_then(|error_code| {
                de_string(&self[6..]).and_then(|(error_message, bytes)| {
                    bytes.to_vec().into_protocol_type().map(|coordinator: Coordinator| FindCoordinatorResponse {
                        throttle_time_ms,
                        error_code,
                        error_message,
                        coordinator,
                    })
                })
            })
        })
    }
}

impl ProtocolDeserializable<Coordinator> for Vec<u8> {
    fn into_protocol_type(self) -> ProtocolDeserializeResult<Coordinator> {
        de_i32(&self[0..=3]).and_then(|node_id| {
            de_string(&self[4..]).and_then(|(host, bytes)| {
                host.ok_or(DeserializeError::of("Unexpected null host for Coordinator"))
                    .and_then(|host| de_i32(&bytes[0..=3]).map(|port| Coordinator { node_id, host, port }))
            })
        })
    }
}
