use thiserror::Error;

#[derive(Error, Debug, PartialEq)]
pub enum KvError {
    #[error("Command is invalid: `{0}`")]
    InvalidCommand(String),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Not found for table: {0}, key: {1}")]
    NotFound(String, String),

    #[error("Fail to encode protobuf message")]
    EncodeError(#[from] prost::EncodeError),

    #[error("Fail to decode protobuf message")]
    DecodeError(#[from] prost::DecodeError),

    #[error("Failed to access sled db")]
    SledError(#[from] sled::Error),
}
