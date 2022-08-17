use thiserror::Error;

#[derive(Error, Debug)]
pub enum KvError {
    #[error("Command is invalid: `{0}`")]
    InvalidCommand(String),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Not found for table: {0}, key: {1}")]
    NotFound(String, String),
}
