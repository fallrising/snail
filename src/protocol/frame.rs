use bytes::Bytes;

#[derive(Debug, Clone)]
pub struct Frame {
    pub args: Vec<Bytes>,
}

#[derive(Debug, Clone)]
pub enum Reply {
    Ok,
    Simple(String),
    Int(i64),
    Bulk(Bytes),
    NullBulk,
    Array(Vec<Reply>),
    NullArray,
    Err(CommandErrKind, String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandErrKind {
    Generic,
    WrongType,
}