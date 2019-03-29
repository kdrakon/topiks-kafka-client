use crate::kafka_protocol::api_verification::KafkaApiVersioned;
use crate::kafka_protocol::protocol_primitives::*;
use crate::kafka_protocol::protocol_serializable::*;

#[derive(Clone, Debug)]
pub struct AlterConfigsRequest {
    pub resources: Vec<Resource>,
    pub validate_only: bool,
}

#[derive(Clone, Debug)]
pub struct Resource {
    pub resource_type: i8,
    pub resource_name: String,
    pub config_entries: Vec<ConfigEntry>,
}

#[derive(Clone, Debug)]
pub struct ConfigEntry {
    pub config_name: String,
    pub config_value: Option<String>,
}

impl KafkaApiVersioned for AlterConfigsRequest {
    fn api_key() -> i16 {
        33
    }
    fn version() -> i16 {
        0
    }
}

impl ProtocolSerializable for AlterConfigsRequest {
    fn into_protocol_bytes(self) -> ProtocolSerializeResult {
        let resources = self.resources;
        let validate_only = self.validate_only;
        resources.into_protocol_bytes().and_then(|resources| {
            ProtocolPrimitives::Boolean(validate_only).into_protocol_bytes().map(|validate_only| [resources, validate_only].concat())
        })
    }
}

impl ProtocolSerializable for Resource {
    fn into_protocol_bytes(self) -> ProtocolSerializeResult {
        let resource_type = self.resource_type;
        let resource_name = self.resource_name;
        let config_entries = self.config_entries;
        ProtocolPrimitives::I8(resource_type).into_protocol_bytes().and_then(|resource_type| {
            resource_name.into_protocol_bytes().and_then(|resource_name| {
                config_entries.into_protocol_bytes().map(|config_entries| [resource_type, resource_name, config_entries].concat())
            })
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
