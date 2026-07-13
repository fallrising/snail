use std::fmt;

use crate::protocol::encoder;

#[derive(Debug, Clone)]
pub enum ProtocolError {
    InvalidPrefix(u8),
    InvalidLength,
    Overflow,
    TooLarge(&'static str),
    MissingCrlf,
    Incomplete,
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPrefix(b) => write!(f, "unexpected byte 0x{b:02x}"),
            Self::InvalidLength => write!(f, "invalid length"),
            Self::Overflow => write!(f, "integer overflow"),
            Self::TooLarge(what) => write!(f, "{what} exceeds limit"),
            Self::MissingCrlf => write!(f, "expected CRLF"),
            Self::Incomplete => write!(f, "incomplete frame"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum CommandError {
    UnknownCommand(String),
    WrongArity(&'static str),
    WrongType,
    NotInteger,
    NotFloat,
    Syntax,
    Oom,
    CrossSlot,
    InvalidDb,
    Resp3NotSupported,
    UnknownSubcommand(String),
}

impl CommandError {
    pub fn to_resp(&self) -> String {
        match self {
            Self::UnknownCommand(name) => format!("unknown command '{name}'"),
            Self::WrongArity(name) => format!("wrong number of arguments for '{name}' command"),
            Self::WrongType => {
                "Operation against a key holding the wrong kind of value".into()
            }
            Self::NotInteger => "value is not an integer or out of range".into(),
            Self::NotFloat => "value is not a valid float".into(),
            Self::Syntax => "syntax error".into(),
            Self::Oom => "command not allowed when used memory > 'maxmemory'".into(),
            Self::CrossSlot => {
                "Keys in request don't hash to the same slot".into()
            }
            Self::InvalidDb => "DB index is out of range".into(),
            Self::Resp3NotSupported => "RESP3 is not supported".into(),
            Self::UnknownSubcommand(s) => format!("Unknown subcommand or wrong number of arguments for '{s}'"),
        }
    }

    pub fn is_wrongtype(&self) -> bool {
        matches!(self, Self::WrongType)
    }
}

#[derive(Debug)]
pub enum ServerError {
    Config(String),
    Io(std::io::Error),
    AddrParse(std::net::AddrParseError),
    Bootstrap(String),
}

impl From<std::io::Error> for ServerError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<std::net::AddrParseError> for ServerError {
    fn from(e: std::net::AddrParseError) -> Self {
        Self::AddrParse(e)
    }
}

impl From<toml::de::Error> for ServerError {
    fn from(e: toml::de::Error) -> Self {
        Self::Config(e.to_string())
    }
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(s) => write!(f, "config error: {s}"),
            Self::Io(e) => write!(f, "io error: {e}"),
            Self::AddrParse(e) => write!(f, "addr parse error: {e}"),
            Self::Bootstrap(s) => write!(f, "bootstrap error: {s}"),
        }
    }
}

impl std::error::Error for ServerError {}

pub fn protocol_err_reply(err: &ProtocolError) -> String {
    encoder::format_err(&format!("Protocol error: {err}"))
}

pub fn command_err_reply(err: &CommandError) -> String {
    if err.is_wrongtype() {
        encoder::format_wrongtype()
    } else {
        encoder::format_err(&err.to_resp())
    }
}