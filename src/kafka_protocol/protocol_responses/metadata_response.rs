extern crate byteorder;

use crate::kafka_protocol::protocol_serializable::*;

#[derive(Debug, Clone)]
pub struct MetadataResponse {
    pub throttle_time_ms: i32,
    pub brokers: Vec<BrokerMetadata>,
    pub cluster_id: Option<String>,
    pub controller_id: i32,
    pub topic_metadata: Vec<TopicMetadata>,
}

#[derive(Debug, Clone)]
pub struct BrokerMetadata {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TopicMetadata {
    pub error_code: i16,
    pub topic: String,
    pub is_internal: bool,
    pub partition_metadata: Vec<PartitionMetadata>,
}

#[derive(Debug, Clone)]
pub struct PartitionMetadata {
    pub error_code: i16,
    pub partition: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    pub offline_replicas: Vec<i32>,
}

impl ProtocolDeserializable<MetadataResponse> for Vec<u8> {
    fn into_protocol_type(self) -> ProtocolDeserializeResult<MetadataResponse> {
        let result = de_i32(&self[0..4]).and_then(|throttle_time_ms| {
            de_array(&self[4..], deserialize_broker_metadata).map(|(brokers, remaining_bytes)| (throttle_time_ms, brokers, remaining_bytes))
        });

        let result = result.and_then(|(throttle_time_ms, brokers, remaining_bytes)| {
            de_string(&remaining_bytes).map(|(cluster_id, remaining_bytes)| (throttle_time_ms, brokers, cluster_id, remaining_bytes))
        });

        let result = result.and_then(|(throttle_time_ms, brokers, cluster_id, remaining_bytes)| {
            de_i32(&remaining_bytes[0..4]).map(|controller_id| (throttle_time_ms, brokers, cluster_id, controller_id, &remaining_bytes[4..]))
        });

        let result = result.and_then(|(throttle_time_ms, brokers, cluster_id, controller_id, remaining_bytes)| {
            de_array(&remaining_bytes, deserialize_topic_metadata)
                .map(|(topic_metadata, remaining_bytes)| (throttle_time_ms, brokers, cluster_id, controller_id, topic_metadata, remaining_bytes))
        });

        result.map(|(throttle_time_ms, brokers, cluster_id, controller_id, topic_metadata, remaining_bytes)| {
            if !remaining_bytes.is_empty() {
                panic!("Deserialize of MetadataResponse did not cover remaining {} bytes", remaining_bytes.len());
            }
            MetadataResponse { throttle_time_ms, brokers, cluster_id, controller_id, topic_metadata }
        })
    }
}

fn deserialize_broker_metadata(bytes: &[u8]) -> ProtocolDeserializeResult<DynamicSize<BrokerMetadata>> {
    let result = de_i32(&bytes[0..4]).and_then(|node_id| {
        de_string(&bytes[4..]).map(|(host, remaining_bytes)| {
            let host = host.expect("Expected host string");
            (node_id, host, remaining_bytes)
        })
    });

    let result = result.and_then(|(node_id, host, remaining_bytes)| {
        de_i32(&remaining_bytes[0..4])
            .and_then(|port| de_string(&remaining_bytes[4..]).map(|(rack, remaining_bytes)| (node_id, host, port, rack, remaining_bytes)))
    });

    result.map(|(node_id, host, port, rack, remaining_bytes)| (BrokerMetadata { node_id, host, port, rack }, remaining_bytes))
}

fn deserialize_topic_metadata(bytes: &[u8]) -> ProtocolDeserializeResult<DynamicSize<TopicMetadata>> {
    de_i16(&bytes[0..2]).and_then(|error_code| {
        de_string(&bytes[2..])
            .map(|(topic, remaining_bytes)| {
                let topic = topic.expect("Unexpected empty topic name");
                let is_internal = if remaining_bytes[0] == 1 { true } else { false };
                (topic, is_internal, &remaining_bytes[1..])
            })
            .and_then(|(topic, is_internal, remaining_bytes)| {
                de_array(&remaining_bytes, deserialize_partition_metadata).map(|(partition_metadata, remaining_bytes)| {
                    (TopicMetadata { error_code, topic, is_internal, partition_metadata }, remaining_bytes)
                })
            })
    })
}

fn deserialize_partition_metadata(bytes: &[u8]) -> ProtocolDeserializeResult<DynamicSize<PartitionMetadata>> {
    de_i16(&bytes[0..2]).and_then(|error_code| {
        de_i32(&bytes[2..6]).and_then(|partition| {
            de_i32(&bytes[6..10]).and_then(|leader| {
                de_array(&bytes[10..], |bytes| de_i32(&bytes[0..4]).map(|replicas| (replicas, &bytes[4..]))).and_then(
                    |(replicas, remaining_bytes)| {
                        de_array(remaining_bytes, |bytes| de_i32(&bytes[0..4]).map(|isr| (isr, &bytes[4..]))).and_then(|(isr, remaining_bytes)| {
                            de_array(remaining_bytes, |bytes| de_i32(&bytes[0..4]).map(|offline_replicas| (offline_replicas, &bytes[4..]))).map(
                                |(offline_replicas, remaining_bytes)| {
                                    (PartitionMetadata { error_code, partition, leader, replicas, isr, offline_replicas }, remaining_bytes)
                                },
                            )
                        })
                    },
                )
            })
        })
    })
}
