use std::error::Error;
use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;
use std::io::{Cursor, Read, Write};
use std::net::*;

use byteorder::{BigEndian, ReadBytesExt};
use native_tls::TlsConnector;

use kafka_protocol::protocol_request::*;
use kafka_protocol::protocol_response::*;
use kafka_protocol::protocol_serializable::*;
use util::io::IO;
use BootstrapServer;

#[derive(Debug)]
pub struct TcpRequestError {
    pub error: String,
}

impl Display for TcpRequestError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "TCP Request Error: {}", self.error)
    }
}

impl TcpRequestError {
    pub fn of(error: String) -> TcpRequestError {
        TcpRequestError { error }
    }
    pub fn from(error: &str) -> TcpRequestError {
        TcpRequestError::of(String::from(error))
    }
}

pub trait ApiClientTrait {
    fn request<T, U>(&self, bootstrap_server: &BootstrapServer, request: Request<T>) -> Result<Response<U>, TcpRequestError>
    where
        T: ProtocolSerializable,
        Vec<u8>: ProtocolDeserializable<Response<U>>;
}

pub type ApiClientProvider<T> = Box<Fn() -> IO<T, TcpRequestError>>;

#[derive(Clone)]
pub struct ApiClient {}

impl ApiClient {
    pub fn new() -> ApiClient {
        ApiClient {}
    }
}

impl ApiClientTrait for ApiClient {
    fn request<T, U>(&self, bootstrap_server: &BootstrapServer, request: Request<T>) -> Result<Response<U>, TcpRequestError>
    where
        T: ProtocolSerializable,
        Vec<u8>: ProtocolDeserializable<Response<U>>,
    {
        let response = request.into_protocol_bytes().and_then(|bytes| {
            let tls_connector = TlsConnector::new().unwrap();

            TcpStream::connect(bootstrap_server.socket_addr()).and_then(|mut stream| {
                //let mut stream = tls_connector.connect(bootstrap_server.domain(), stream).unwrap();
                stream.write(bytes.as_slice()).and_then(|_| {
                    let mut result_size_buf: [u8; 4] = [0; 4];
                    stream.read(&mut result_size_buf).and_then(|_| Cursor::new(result_size_buf.to_vec()).read_i32::<BigEndian>()).and_then(
                        |result_size| {
                            let mut message_buf: Vec<u8> = vec![0; result_size as usize];
                            stream.read_exact(&mut message_buf).map(|_| message_buf)
                        },
                    )
                })
            })
        });

        response.map_err(|e| TcpRequestError::of(format!("{}", e.description()))).and_then(|bytes| {
            //            println!("bytes: {:?}", utils::to_hex_array(&bytes));
            bytes.into_protocol_type().map_err(|e| TcpRequestError::of(e.error))
        })
    }
}
