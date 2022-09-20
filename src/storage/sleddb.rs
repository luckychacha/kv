use sled::{Db, IVec};
use std::path::Path;
use std::convert::TryInto;
use std::str;

use crate::{Storage, Kvpair, KvError};

#[derive(Debug)]
pub struct SledDb(Db);

impl SledDb {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self(sled::open(path).unwrap())
    }

    fn get_full_key(table: &str, key: &str) -> String {
        format!("{}: {}", table, key)
    }

    fn get_table_prefix(table: &str) -> String {
        format!("{}:", table)
    }
}

/// 把 Option<Result<T, E>> flip 成为 Result<Option<T>, E>
/// 从这个函数里可以看到函数式编程的优美
fn flip<T, E>(x: Option<Result<T, E>>) -> Result<Option<T>, E> {
    // x 是 Option 类型，所以 x 可能是 Some(Result<T, E>) 也可能是 None
    // x 为 None 时，走 map_or 的 default，
    //  需要返回 Result 类型，也就是可能是 Ok，也可能是 Err
    //  此处返回 Ok(None)
    // x 为 Some(Result<T, E>) 时，需要返回的是 Result<Option<T>, E>
    // 所以对使用 map 函数，参数 op 为 Some，
    //  当 x 是 Err 时，直接返回 E
    //  当 x 是 Ok 时，返回 Some(v)
    x.map_or(Ok(None), |v| v.map(Some))
}

impl Storage for SledDb {
    fn get(&self, table: &str, key: &str) -> Result<Option<crate::Value>, KvError> {
        let name = SledDb::get_full_key(table, key);
        let result = self.0.get(name.as_bytes())?.map(|v| v.as_ref().try_into());

        flip(result)

    }

    fn set(&self, table: &str, key: String, value: crate::Value) -> Result<Option<crate::Value>, crate::KvError> {
        todo!()
    }

    fn contains(&self, table: &str, key: &str) -> Result<bool, crate::KvError> {
        todo!()
    }

    fn del(&self, table: &str, key: &str) -> Result<Option<crate::Value>, crate::KvError> {
        todo!()
    }

    fn get_all(&self, table: &str) -> Result<Vec<crate::Kvpair>, crate::KvError> {
        todo!()
    }

    fn get_iter(&self, table: &str) -> Result<Box<dyn Iterator<Item = crate::Kvpair>>, crate::KvError> {
        todo!()
    }
}

impl From<Result<(IVec, IVec), sled::Error>> for Kvpair {
    fn from(v: Result<(IVec, IVec), sled::Error>) -> Self {
        match v {
            Ok((k, v)) => match v.as_ref().try_into() {
                Ok(v) => Kvpair::new(ivec_to_key(k.as_ref()), v),
                Err(_) => Kvpair::default(),
            }
            _ => Kvpair::default(),
        }
    }
}

fn ivec_to_key(ivec: &[u8]) -> &str {
    let s = str::from_utf8(ivec).unwrap();
    let mut iter = s.split(':');
    iter.next();
    iter.next().unwrap()
}