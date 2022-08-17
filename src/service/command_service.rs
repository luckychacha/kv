use crate::{CommandResponse, CommandService, Hget, KvError, Storage};

impl CommandService for Hget {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.get(&self.table, &self.key) {
            Ok(Some(v)) => v.into(),
            Ok(None) => KvError::NotFound(self.table, self.key).into(),
            Err(_) => todo!(),
        }
    }
}
