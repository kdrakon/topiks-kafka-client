extern crate byteorder;

use self::byteorder::{BigEndian, ReadBytesExt};
use std::error::Error;
use std::io::*;
use super::protocol_primitives::*;
use super::protocol_serializable::*;

/// Top-level response which can be sent from a Kafka broker.
///
pub struct Response<T> {
    pub header: ResponseHeader,
    pub response_message: T,
}

/// Header information for a Response
///
pub struct ResponseHeader {
    pub correlation_id: i32
}

impl ProtocolDeserializable<ResponseHeader> for Vec<u8> {
    fn into_protocol_type(self) -> ProtocolDeserializeResult<ResponseHeader> {
        de_i32(self).map(|correlation_id| {
            ResponseHeader { correlation_id }
        })
    }
}

/// MetadataResponse
///
pub struct MetadataResponse {
    throttle_time_ms: i32,
    brokers: Vec<BrokerMetadata>,
    cluster_id: Option<String>,
    controller_id: i32,
    topic_metadata: Vec<TopicMetadata>,
}

pub struct BrokerMetadata {
    node_id: i32,
    host: String,
    port: i32,
    rack: Option<String>,
}

pub struct TopicMetadata {
    error_code: i16,
    topic: String,
    is_internal: bool,
    partition_metadata: Vec<PartitionMetadata>,
}

pub struct PartitionMetadata {
    error_code: i16,
    partition: i32,
    leader: i32,
    replicas: Vec<i32>,
    isr: Vec<i32>,
    offline_replicas: Vec<i32>,
}

impl ProtocolDeserializable<Response<MetadataResponse>> for Vec<u8> {
    fn into_protocol_type(self) -> ProtocolDeserializeResult<Response<MetadataResponse>> {
        let fields: ProtocolDeserializeResult<(ResponseHeader, MetadataResponse)> =
            self[0..3].to_vec().into_protocol_type().and_then(|header| {
                self[4..].to_vec().into_protocol_type().map(|response_message| {
                    (header, response_message)
                })
            });

        fields.map(|(header, response_message)| {
            Response {
                header,
                response_message,
            }
        })
    }
}

impl ProtocolDeserializable<MetadataResponse> for Vec<u8> {
    fn into_protocol_type(self) -> ProtocolDeserializeResult<MetadataResponse> {
//        de_i32(self[0..3].to_vec()).and_then(|throttle_time_ms| {
//            de_array(self[4..].to_vec(), deserialize_broker_metadata)
//        }).and_then(|(brokers, remaining_bytes)| {
//
//            unimplemented!()
//        });
        unimplemented!()
    }
}

fn deserialize_broker_metadata(bytes: Vec<u8>) -> (BrokerMetadata, Vec<u8>) {
    unimplemented!()
}
