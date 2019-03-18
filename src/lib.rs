extern crate byteorder;
extern crate native_tls;
#[cfg(test)]
#[macro_use]
extern crate proptest;

pub mod api_client;
pub mod kafka_protocol;

#[derive(Clone, Debug)]
pub struct KafkaServerAddr {
    pub domain: String,
    pub port: i32,
    pub use_tls: bool,
}

impl KafkaServerAddr {
    pub fn of(domain: String, port: i32, use_tls: bool) -> KafkaServerAddr {
        KafkaServerAddr { domain, port, use_tls }
    }
    pub fn from_arg(addr_arg: &str, use_tls: bool) -> Option<KafkaServerAddr> {
        let split: Vec<&str> = addr_arg.split(':').collect::<Vec<&str>>();
        match split.as_slice() {
            [domain_ip, port] => port.parse::<i32>().ok().map(|port| KafkaServerAddr::of(String::from(*domain_ip), port, use_tls)),
            _ => None,
        }
    }
    pub fn as_socket_addr(&self) -> String {
        format!("{}:{}", self.domain, self.port)
    }
}

fn to_hex_array(bytes: &Vec<u8>) -> Vec<String> {
    bytes.iter().cloned().map(|b| format!("0x{:02X}", b)).collect::<Vec<String>>()
}

type Thunk<A, E> = Box<Fn() -> Result<A, E>>;

pub struct IO<A, E> {
    thunk: Thunk<A, E>,
}

impl<A: 'static, E: 'static> IO<A, E> {
    pub fn new(f: Box<Fn() -> Result<A, E>>) -> IO<A, E> {
        IO { thunk: f }
    }

    pub fn into_result(self) -> Result<A, E> {
        (self.thunk)()
    }

    #[inline]
    pub fn map<B: 'static>(self, f: Box<Fn(A) -> B>) -> IO<B, E> {
        IO {
            thunk: Box::new(move || {
                let run: Result<A, E> = (self.thunk)();
                match run {
                    Ok(a) => Ok(f(a)),
                    Err(e) => Err(e),
                }
            }),
        }
    }

    #[inline]
    pub fn and_then<B: 'static>(self, f: Box<Fn(A) -> IO<B, E>>) -> IO<B, E> {
        IO {
            thunk: Box::new(move || {
                let run: Result<A, E> = (self.thunk)();
                match run {
                    Ok(a) => {
                        let io: IO<B, E> = f(a);
                        io.into_result()
                    }
                    Err(e) => Err(e),
                }
            }),
        }
    }

    #[inline]
    pub fn and_then_result<B: 'static>(self, f: Box<Fn(A) -> Result<B, E>>) -> IO<B, E> {
        IO {
            thunk: Box::new(move || {
                let run: Result<A, E> = (self.thunk)();
                match run {
                    Ok(a) => f(a),
                    Err(e) => Err(e),
                }
            }),
        }
    }
}

pub fn foo() -> Result<String, String> {
    let external_io = Box::new(|| Ok(String::from("coo coo coo")));
    let io: IO<i32, String> = IO::new(Box::new(|| Ok(42)));

    let io2: IO<String, String> = io.map(Box::new(|i: i32| i * 2)).and_then(Box::new(move |_i: i32| IO::new(external_io.clone())));

    io2.into_result()
}
