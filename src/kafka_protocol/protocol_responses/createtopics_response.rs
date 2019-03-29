extern crate byteorder;

use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;

use crate::kafka_protocol::protocol_serializable::*;

pub struct CreateTopicsResponse {
    pub topic_errors: Vec<TopicError>,
}

#[derive(Debug)]
pub struct TopicError {
    pub topic: String,
    pub error_code: i16,
    pub error_message: Option<String>,
}

impl Display for TopicError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Topic Error for {}: {}", self.topic, self.error_code)
    }
}

impl ProtocolDeserializable<CreateTopicsResponse> for Vec<u8> {
    fn into_protocol_type(self) -> ProtocolDeserializeResult<CreateTopicsResponse> {
        de_array(&self, deserialize_topic_errors).map(|(topic_errors, bytes)| {
            if !bytes.is_empty() {
                panic!("Unexpected bytes deserializing CreateTopicsResponse")
            } else {
                CreateTopicsResponse { topic_errors }
            }
        })
    }
}

fn deserialize_topic_errors(bytes: &[u8]) -> ProtocolDeserializeResult<DynamicSize<TopicError>> {
    de_string(&bytes).and_then(|(topic, remaining_bytes)| {
        let topic = topic.expect("Unexpected missing topic name in TopicError");
        de_i16(&remaining_bytes[0..2]).and_then(|error_code| {
            de_string(&remaining_bytes[2..])
                .map(|(error_message, remaining_bytes)| (TopicError { topic, error_code, error_message }, remaining_bytes))
        })
    })
}
