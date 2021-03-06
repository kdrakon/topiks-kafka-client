use crate::kafka_protocol::api_verification::KafkaApiVersioned;
use crate::kafka_protocol::protocol_primitives::ProtocolPrimitives::*;
use crate::kafka_protocol::protocol_serializable::ProtocolSerializeResult;
use crate::kafka_protocol::protocol_serializable::*;

#[derive(Clone)]
pub struct MetadataRequest {
    pub topics: Option<Vec<String>>,
    pub allow_auto_topic_creation: bool,
}

impl KafkaApiVersioned for MetadataRequest {
    fn api_key() -> i16 {
        3
    }
    fn version() -> i16 {
        5
    }
}

impl ProtocolSerializable for MetadataRequest {
    fn into_protocol_bytes(self) -> ProtocolSerializeResult {
        let topics = self.topics;
        let allow_auto_topic_creation = Boolean(self.allow_auto_topic_creation);
        topics.into_protocol_bytes().and_then(|mut topic_bytes| {
            allow_auto_topic_creation.into_protocol_bytes().map(|ref mut a| {
                topic_bytes.append(a);
                topic_bytes
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::kafka_protocol::protocol_request::*;

    use super::*;

    #[test]
    fn verify_request() {
        let topics: Option<Vec<String>> = Some(vec![]);
        let metadata_request = MetadataRequest { topics, allow_auto_topic_creation: false };

        let request: Request<MetadataRequest> = Request {
            header: RequestHeader { api_key: 3, api_version: 5, correlation_id: 42, client_id: String::from("sean") },
            request_message: metadata_request,
        };

        let bytes = request.into_protocol_bytes().unwrap();
        assert_eq!(vec![0, 0, 0, 19, 0, 3, 0, 5, 0, 0, 0, 42, 0, 4, 115, 101, 97, 110, 0, 0, 0, 0, 0], bytes);
        println!("{:?}", bytes);
    }

    #[test]
    fn verify_metadata_request() {
        let topics = Some(vec![String::from("my_kafka_topic_1"), String::from("my_kafka_topic_2")]);
        let metadata_request = MetadataRequest { topics, allow_auto_topic_creation: true };
        let expected: Vec<u8> = vec![
            0, 0, 0, 2, 0, 16, 0x6D, 0x79, 0x5F, 0x6B, 0x61, 0x66, 0x6B, 0x61, 0x5F, 0x74, 0x6F, 0x70, 0x69, 0x63, 0x5F, 0x31, 0, 16, 0x6D, 0x79,
            0x5F, 0x6B, 0x61, 0x66, 0x6B, 0x61, 0x5F, 0x74, 0x6F, 0x70, 0x69, 0x63, 0x5F, 0x32, 1,
        ];
        assert_eq!(expected, metadata_request.into_protocol_bytes().unwrap());
    }
}
