use crate::kafka_protocol::api_verification::KafkaApiVersioned;
use crate::kafka_protocol::protocol_primitives::ProtocolPrimitives;
use crate::kafka_protocol::protocol_primitives::ProtocolPrimitives::*;
use crate::kafka_protocol::protocol_serializable::ProtocolSerializeResult;
use crate::kafka_protocol::protocol_serializable::*;

#[derive(Clone)]
pub struct CreateTopicsRequest {
    pub create_topic_requests: Vec<Request>,
    pub timeout: i32,
    pub validate_only: bool,
}

impl KafkaApiVersioned for CreateTopicsRequest {
    fn api_key() -> i16 {
        19
    }
    fn version() -> i16 {
        1
    }
}

#[derive(Clone)]
pub struct Request {
    pub topic: String,
    pub num_partitions: i32,
    pub replication_factor: i16,
    pub replica_assignments: Vec<ReplicaAssignment>,
    pub config_entries: Vec<ConfigEntry>,
}

#[derive(Clone)]
pub struct ReplicaAssignment {
    pub partition: i32,
    pub replicas: Vec<i32>,
}

#[derive(Clone)]
pub struct ConfigEntry {
    pub config_name: String,
    pub config_value: Option<String>,
}

impl ProtocolSerializable for CreateTopicsRequest {
    fn into_protocol_bytes(self) -> ProtocolSerializeResult {
        let create_topic_requests = self.create_topic_requests;
        let timeout = I32(self.timeout);
        let validate_only_byte: [u8; 1] = if self.validate_only { [1] } else { [0] };
        create_topic_requests
            .into_protocol_bytes()
            .and_then(|topic_requests| timeout.into_protocol_bytes().map(|timeout| [topic_requests, timeout, validate_only_byte.to_vec()].concat()))
    }
}

impl ProtocolSerializable for Request {
    fn into_protocol_bytes(self) -> ProtocolSerializeResult {
        let topic = self.topic;
        let num_partitions = self.num_partitions;
        let replication_factor = self.replication_factor;
        let replica_assignments = self.replica_assignments;
        let config_entries = self.config_entries;

        topic.into_protocol_bytes().and_then(|topic| {
            I32(num_partitions).into_protocol_bytes().and_then(|num_partitions| {
                I16(replication_factor).into_protocol_bytes().and_then(|replication_factor| {
                    replica_assignments.into_protocol_bytes().and_then(|replica_assignments| {
                        config_entries
                            .into_protocol_bytes()
                            .map(|config_entries| [topic, num_partitions, replication_factor, replica_assignments, config_entries].concat())
                    })
                })
            })
        })
    }
}

impl ProtocolSerializable for ReplicaAssignment {
    fn into_protocol_bytes(self) -> ProtocolSerializeResult {
        I32(self.partition).into_protocol_bytes().and_then(|partition| {
            self.replicas
                .into_iter()
                .map(|r| I32(r))
                .collect::<Vec<ProtocolPrimitives>>()
                .into_protocol_bytes()
                .map(|replicas| [partition, replicas].concat())
        })
    }
}

impl ProtocolSerializable for ConfigEntry {
    fn into_protocol_bytes(self) -> ProtocolSerializeResult {
        let config_name = self.config_name;
        let config_value = self.config_value;
        config_name
            .into_protocol_bytes()
            .and_then(|config_name| config_value.into_protocol_bytes().map(|config_value| [config_name, config_value].concat()))
    }
}

#[cfg(test)]
mod tests {
    use crate::kafka_protocol::protocol_requests::createtopics_request::*;

    proptest! {
        #[test]
        fn verify_serde_for_createtopics_request(ref topic in ".*") {
            let request = CreateTopicsRequest {
                create_topic_requests: vec![Request { topic: topic.clone(), num_partitions: 16,  replication_factor: 3, replica_assignments: vec![], config_entries: vec![] }],
                timeout: 42,
                validate_only: false
            };
            match request.into_protocol_bytes() {
                Ok(_bytes) => (),
                Err(e) => panic!(e)
            };
        }
    }
}
