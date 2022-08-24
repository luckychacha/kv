mod command_service;

use std::sync::Arc;

use crate::{command_request::RequestData, CommandRequest, CommandResponse, KvError, Storage, MemTable};

// 对 Command 的处理的抽象
pub trait CommandService {
    // 处理 Command，返回 Response
    fn execute(self, store: &impl Storage) -> CommandResponse;
}


// Service 数据结构
pub struct Service<Store = MemTable> {
    inner: Arc<ServiceInner<Store>>,
}

impl<Store: Storage> Service<Store> {
    pub fn new(store: Store) -> Self {
        Self {
            inner: Arc::new( ServiceInner::new(store) ),
        }
    }
}

// Service 内部数据结构
pub struct ServiceInner<Store> {
    store: Store,
}

// 从 Request 中得到 Response，目前处理 HGET/HGETALL/HSET
pub fn dispatch(cmd: CommandRequest, store: &impl Storage) -> CommandResponse {
    match cmd.request_data {
        Some(RequestData::Hget(param)) => param.execute(store),
        Some(RequestData::Hgetall(param)) => param.execute(store),
        Some(RequestData::Hset(param)) => param.execute(store),
        None => KvError::InvalidCommand("Request has no data".into()).into(),
        _ => KvError::Internal("Not implemented".into()).into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MemTable, Value};

    #[test]
    fn service_should_works() {
        let service = Service::new(MemTable::new())
    }
}