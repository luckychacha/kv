use crate::{
    dispatch, CommandRequest, CommandResponse, CommandService, Hget, Hgetall, Hset, KvError,
    Kvpair, MemTable, Storage, Value,
};

impl CommandService for Hget {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.get(&self.table, &self.key) {
            Ok(Some(v)) => v.into(),
            Ok(None) => KvError::NotFound(self.table, self.key).into(),
            Err(_) => todo!(),
        }
    }
}

impl CommandService for Hgetall {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.get_all(&self.table) {
            Ok(v) => v.into(),
            Err(e) => e.into(),
        }
    }
}

impl CommandService for Hset {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match self.pair {
            Some(v) => match store.set(&self.table, v.key, v.value.unwrap_or_default()) {
                Ok(Some(v)) => v.into(),
                Ok(None) => Value::default().into(),
                Err(e) => e.into(),
            },
            None => KvError::InvalidCommand(format!("{:?}", self)).into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{assert_res_error, assert_res_ok};

    use super::*;

    #[test]
    fn hset_should_work() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hset("t1", "hello", "world".into());
        let res = dispatch(cmd.clone(), &store);

        assert_res_ok(res, &[Value::default()], &[]);
    }

    #[test]
    fn hget_should_work() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hset("score", "u1", 10.into());
        dispatch(cmd, &store);
        let cmd = CommandRequest::new_hget("score", "u1");
        let res = dispatch(cmd, &store);
        assert_res_ok(res, &[10.into()], &[]);
    }

    #[test]
    fn hget_with_non_exist_key_should_return_404() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hget("score", "ui");
        let res = dispatch(cmd, &store);
        assert_res_error(res, 404, "Not found");
    }

    #[test]
    fn hget_all_should_work() {
        let store = MemTable::new();
        let commands = vec![
            CommandRequest::new_hset("t1", "k1", 10.into()),
            CommandRequest::new_hset("t1", "k2", 5.into()),
            CommandRequest::new_hset("t1", "k3", 6.into()),
            CommandRequest::new_hset("t1", "k1", 9.into()),
        ];
        for command in commands {
            dispatch(command, &store);
        }

        let hget_all_command = CommandRequest::new_hget_all("t1");
        let res = dispatch(hget_all_command, &store);

        let pairs = [
            Kvpair::new("k1", 9.into()),
            Kvpair::new("k2", 5.into()),
            Kvpair::new("k3", 6.into()),
        ];

        assert_res_ok(res, &[], &pairs);
    }
}
