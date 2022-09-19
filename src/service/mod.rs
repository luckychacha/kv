mod command_service;

use std::sync::Arc;

use tracing::debug;

use crate::{
    command_request::RequestData, CommandRequest, CommandResponse, KvError, Kvpair, MemTable,
    Storage, Value,
};

// 对 Command 的处理的抽象
pub trait CommandService {
    // 处理 Command，返回 Response
    fn execute(self, store: &impl Storage) -> CommandResponse;
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

// 默认泛型类型参数是 MemTable
pub struct Service<Store: Storage = MemTable> {
    inner: Arc<ServiceInner<Store>>,
}

impl<Store: Storage> Clone for Service<Store> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<Store: Storage> Service<Store> {
    pub fn new(store: Store) -> Self {
        Self {
            inner: Arc::new(ServiceInner { store }),
        }
    }

    pub fn execute(&self, cmd: CommandRequest) -> CommandResponse {
        debug!("cmd is: {:?}", cmd);
        let res = dispatch(cmd, &self.inner.store);
        debug!("cmd dispatch result is: {:?}", res);
        res
    }
}

struct ServiceInner<Storage> {
    store: Storage,
    on_received: Vec<fn(&CommandRequest)>,
    on_executed: Vec<fn(&CommandResponse)>,
    on_before_send: Vec<fn(&CommandResponse)>,
    on_after_send: Vec<fn()>,
}

#[cfg(test)]
mod tests {
    use std::thread;
    use http::StatusCode;
    use tracing::info;

    use super::*;
    use crate::{MemTable, Value};

    #[test]
    fn service_should_works() {
        // 我们需要一个 service 结构至少包含 Storage
        let service = Service::new(MemTable::new());
        // service 可以运行在多线程环境下，它的 clone 应该是轻量级的
        let cloned = service.clone();
        // 创建一个线程，在 table t1 中写入 k1, v1
        let handle = thread::spawn(move || {
            let res = cloned.execute(CommandRequest::new_hset("t1", "k1", "v1".into()));
            assert_res_ok(res, &[Value::default()], &[]);
        });
        handle.join().unwrap();

        // 在当前线程下读取 table t1 的 k1，应该返回 v1
        let res = service.execute(CommandRequest::new_hget("t1", "k1"));
        assert_res_ok(res, &["v1".into()], &[]);
    }

    #[test]
    fn event_registration_should_work() {
        fn b(cmd: &CommandRequest) {
            info!("Got {:?}", cmd);
        }

        fn c(res: &CommandResponse) {
            info!("{:?}", res);
        }

        fn d(res: &mut CommandResponse) {
            res.status = StatusCode::CREATED.as_u16() as _;
        }

        fn e() {
            info!("Data is send.");
        }

        let service = ServiceInner::new(MemTable::default())
        .fn_received(|_: &CommandRequest| {})
        .fn_received(b)
        .fn_executed(c)
        .fn_before_send(d)
        .fn_after_send(e)
        .into();

        let res = service.execute(CommandRequest::new_hset("t1", "k1", "v1".into()));
        assert_eq!(res.status, StatusCode::CREATED.as_u16() as _);
        assert_eq!(res.message, "");
        assert_eq!(res.values, vec![Value::default()]);

    }


}

// 需要 pub 才能让这个方法被 command_service 调用
#[cfg(test)]
pub fn assert_res_ok(mut res: CommandResponse, values: &[Value], pairs: &[Kvpair]) {
    res.pairs.sort_by(|a, b| a.partial_cmp(b).unwrap());

    assert_eq!(res.status, 200);
    assert_eq!(res.message, "");
    assert_eq!(res.values, values);
    assert_eq!(res.pairs, pairs);
}

#[cfg(test)]
pub fn assert_res_error(res: CommandResponse, code: u32, msg: &str) {
    assert_eq!(res.status, code);
    assert!(res.message.contains(msg));
    assert_eq!(res.values, &[]);
    assert_eq!(res.pairs, &[]);
}
