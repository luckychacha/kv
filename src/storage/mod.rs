mod memory;
use crate::{KvError, Kvpair, Value};
pub use memory::MemTable;

/// 对存储的抽象，我们不关心数据在哪儿，但需要定义外接如何和存储打交道
pub trait Storage {
    /// 从一个 HashTable 里获取一个 key 的 value
    fn get(&self, table: &str, key: &str) -> Result<Option<Value>, KvError>;
    /// 从一个 HashTable 里设置一个 key 的 value，返回旧的 value
    fn set(&self, table: &str, key: String, value: Value) -> Result<Option<Value>, KvError>;
    /// 查看 HashTable 中是否有 key
    fn contains(&self, table: &str, key: &str) -> Result<bool, KvError>;
    /// 从 HashTable 中删除一个 key
    fn del(&self, table: &str, key: &str) -> Result<Option<Value>, KvError>;
    /// 遍历 HashTable，返回所有 kv pair（这个接口不好）
    fn get_all(&self, table: &str) -> Result<Vec<Kvpair>, KvError>;
    /// 遍历 HashTable，返回 kv pair 的 Iterator
    fn get_iter(&self, table: &str) -> Result<Box<dyn Iterator<Item = Kvpair>>, KvError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memtable_basic_interface_should_work() {
        let store = MemTable::new();
        test_basi_interface(store);
    }

    fn test_basi_interface(store: impl Storage) {
        let v = store.set("t1", "hello".into(), "world".into());

        assert!(v.unwrap().is_none());
        let v1 = store.set("t1", "hello".into(), "world1".into());
        assert_eq!(v1, Ok(Some("world".into())));
    }

    #[test]
    fn memtable_iter_should_work() {
        let store = MemTable::new();
        test_get_iter(store);
    }
}
