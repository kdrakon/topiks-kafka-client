use crate::kafka_protocol::api_verification::KafkaApiVersioned;
use crate::kafka_protocol::protocol_primitives::ProtocolPrimitives::*;
use crate::kafka_protocol::protocol_serializable::ProtocolSerializeResult;
use crate::kafka_protocol::protocol_serializable::*;

#[derive(Clone)]
pub struct DeleteTopicsRequest {
    pub topics: Vec<String>,
    pub timeout: i32,
}

impl KafkaApiVersioned for DeleteTopicsRequest {
    fn api_key() -> i16 {
        20
    }
    fn version() -> i16 {
        1
    }
}

impl ProtocolSerializable for DeleteTopicsRequest {
    fn into_protocol_bytes(self) -> ProtocolSerializeResult {
        let topics = self.topics;
        let timeout = I32(self.timeout);
        topics.into_protocol_bytes().and_then(|topics| timeout.into_protocol_bytes().map(|timeout| [topics, timeout].concat()))
    }
}

#[cfg(test)]
mod tests {
    use crate::kafka_protocol::protocol_requests::deletetopics_request::*;

    proptest! {
        #[test]
        fn verify_serde_for_deletetopics_request(ref topic_a in ".*", ref topic_b in ".*") {
            let request = DeleteTopicsRequest {
                topics: vec![topic_a.clone(), topic_b.clone()],
                timeout: 42
            };
            match request.into_protocol_bytes() {
                Ok(_bytes) => (),
                Err(e) => panic!(e)
            };
        }
    }
}
