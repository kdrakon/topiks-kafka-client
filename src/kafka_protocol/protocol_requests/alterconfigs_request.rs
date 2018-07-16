use kafka_protocol::protocol_serializable::*;
use kafka_protocol::protocol_primitives::*;

/// Version 0
///
#[derive(Clone)]
pub struct AlterConfigsRequest {
    pub resources: Vec<Resource>,
    pub validate_only: bool,
}

#[derive(Clone)]
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

impl ProtocolSerializable for AlterConfigsRequest {
    fn into_protocol_bytes(self) -> ProtocolSerializeResult {
        let resources = self.resources;
        let validate_only = self.validate_only;
        resources.into_protocol_bytes().and_then(|mut resources| {
            ProtocolPrimitives::Boolean(validate_only).into_protocol_bytes().map(|ref mut validate_only| {
                resources.append(validate_only);
                resources
            })
        })
    }
}

impl ProtocolSerializable for Resource {
    fn into_protocol_bytes(self) -> ProtocolSerializeResult {
        let resource_type = self.resource_type;
        let resource_name = self.resource_name;
        let config_entries = self.config_entries;
        ProtocolPrimitives::I8(resource_type).into_protocol_bytes().and_then(|mut resource_type| {
            resource_name.into_protocol_bytes().and_then(|ref mut resource_name| {
                config_entries.into_protocol_bytes().map(|ref mut config_entries| {
                    resource_type.append(resource_name);
                    resource_type.append(config_entries);
                    resource_type
                })
            })
        })
    }
}

impl ProtocolSerializable for ConfigEntry {
    fn into_protocol_bytes(self) -> ProtocolSerializeResult {
        let config_name = self.config_name;
        let config_value = self.config_value;
        config_name.into_protocol_bytes().and_then(|mut config_name| {
            config_value.into_protocol_bytes().map(|ref mut config_value| {
                config_name.append(config_value);
                config_name
            })
        })
    }
}