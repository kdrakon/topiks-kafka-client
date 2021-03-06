use crate::kafka_protocol::api_verification::KafkaApiVersioned;
use crate::kafka_protocol::protocol_primitives::ProtocolPrimitives;
use crate::kafka_protocol::protocol_serializable::ProtocolSerializable;
use crate::kafka_protocol::protocol_serializable::ProtocolSerializeResult;

#[derive(Clone)]
pub struct DescribeConfigsRequest {
    pub resources: Vec<Resource>,
    pub include_synonyms: bool,
}

#[derive(Clone)]
pub struct Resource {
    pub resource_type: i8,
    pub resource_name: String,
    pub config_names: Option<Vec<String>>,
}

impl KafkaApiVersioned for DescribeConfigsRequest {
    fn api_key() -> i16 {
        32
    }
    fn version() -> i16 {
        1
    }
}

impl ProtocolSerializable for DescribeConfigsRequest {
    fn into_protocol_bytes(self) -> ProtocolSerializeResult {
        let resources = self.resources;
        let include_synonyms = self.include_synonyms;
        resources.into_protocol_bytes().and_then(|resources| {
            ProtocolPrimitives::Boolean(include_synonyms).into_protocol_bytes().map(|include_synonyms| [resources, include_synonyms].concat())
        })
    }
}

impl ProtocolSerializable for Resource {
    fn into_protocol_bytes(self) -> ProtocolSerializeResult {
        let resource_type = ProtocolPrimitives::I8(self.resource_type);
        let resource_name = self.resource_name;
        let config_names = self.config_names;
        resource_type.into_protocol_bytes().and_then(|resource_type| {
            resource_name.clone().into_protocol_bytes().and_then(|resource_name| {
                config_names.into_protocol_bytes().map(|config_names| [resource_type, resource_name, config_names].concat())
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::kafka_protocol::protocol_requests::describeconfigs_request::*;

    fn verify_serde_for_describeconfigs_request(name_a: String, name_b: String) {
        let resources = vec![
            Resource { resource_type: 1, resource_name: name_a.clone(), config_names: Some(vec![String::from("foo")]) },
            Resource { resource_type: 1, resource_name: name_b, config_names: Some(vec![String::from("bar")]) },
            Resource { resource_type: 1, resource_name: name_a, config_names: None },
        ];
        let request = DescribeConfigsRequest { resources, include_synonyms: false };
        match request.into_protocol_bytes() {
            Ok(_bytes) => (),
            Err(e) => panic!(e),
        }
    }

    proptest! {
        #[test]
        fn verify_serde_for_describeconfigs_request_props(ref name_a in ".*", ref name_b in ".*") {
            verify_serde_for_describeconfigs_request(name_a.to_string(), name_b.to_string())
        }
    }
}
