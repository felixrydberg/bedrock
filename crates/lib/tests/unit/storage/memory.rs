use super::*;
use crate::storage::contract::{Harness, storage_contract};

struct Memory;

impl Harness for Memory {
    type Engine = MemoryEngine;

    fn open(&mut self) -> MemoryEngine {
        MemoryEngine::new("test")
    }

    fn corrupt(&mut self, engine: MemoryEngine, key: &[u8]) -> MemoryEngine {
        engine.corrupt(key);
        engine
    }
}

storage_contract!(Memory);

#[test]
fn exact_stats_and_no_disk() {
    let engine = MemoryEngine::new("test");
    engine.put(b"ab", b"xyz").unwrap();
    engine.put(b"c", b"").unwrap();
    assert_eq!(
        engine.stats().unwrap(),
        StorageStats {
            keys: 2,
            live_bytes: 6
        }
    );
    assert!(matches!(
        engine.disk_usage(),
        Err(StorageError::Unsupported(_))
    ));
}
