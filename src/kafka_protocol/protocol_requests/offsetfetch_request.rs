use kafka_protocol::api_verification::KafkaApiVersioned;
use kafka_protocol::protocol_primitives::ProtocolPrimitives::I32;
use kafka_protocol::protocol_primitives::*;
use kafka_protocol::protocol_serializable::ProtocolSerializable;
use kafka_protocol::protocol_serializable::ProtocolSerializeResult;

#[derive(Debug, Clone)]
pub struct OffsetFetchRequest {
    pub group_id: String,
    pub topics: Vec<Topic>,
}

#[derive(Debug, Clone)]
pub struct Topic {
    pub topic: String,
    pub partitions: Vec<i32>,
}

impl KafkaApiVersioned for OffsetFetchRequest {
    fn api_key() -> i16 {
        9
    }
    fn version() -> i16 {
        3
    }
}

impl ProtocolSerializable for OffsetFetchRequest {
    fn into_protocol_bytes(self) -> ProtocolSerializeResult {
        let group_id = self.group_id;
        let topics = self.topics;
        group_id.into_protocol_bytes().and_then(|group_id| topics.into_protocol_bytes().map(|topics| [group_id, topics].concat()))
    }
}

impl ProtocolSerializable for Topic {
    fn into_protocol_bytes(self) -> ProtocolSerializeResult {
        let topic = self.topic;
        let partitions = self.partitions;
        topic.into_protocol_bytes().and_then(|topic| {
            partitions
                .into_iter()
                .map(|p| I32(p))
                .collect::<Vec<ProtocolPrimitives>>()
                .into_protocol_bytes()
                .map(|partitions| [topic, partitions].concat())
        })
    }
}
