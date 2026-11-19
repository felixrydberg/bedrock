use std::sync::Mutex;

use bedrock_lib::placement::Owner;
use bedrock_lib::storage::memory::MemoryEngine;

use super::*;

fn actor() -> pb::Actor {
    pb::Actor {
        namespace: "ns".into(),
        id: format!("eu_{}a", "c".repeat(25)),
        kind: 0,
    }
}

fn holder() -> pb::Holder {
    pb::Holder {
        cell: "cell-a".into(),
        host: "h".into(),
    }
}

/// A leaving owner's store, read in pages of two records. Before the second page it truncates the actor (as its
/// maintainer may while sealed), or a pushed set drops the range being copied.
struct Leaving {
    source: Arc<Store>,
    target: Arc<Store>,
    pages: Mutex<u32>,
    revoke: bool,
}

#[async_trait]
impl Peers for Leaving {
    async fn sources(&self, _: u64, _: u64) -> Result<Vec<Source>, Status> {
        unreachable!("a handoff copies from the leaving owner")
    }

    async fn slots(&self, _: &pb::Actor) -> Result<[String; actors::SLOTS], Status> {
        unreachable!("a handoff copies every actor")
    }

    async fn page(
        &self,
        _: &Source,
        request: pb::RepairRequest,
    ) -> Result<pb::RepairReply, Status> {
        let page = {
            let mut pages = self.pages.lock().unwrap();
            *pages += 1;
            *pages
        };
        if page == 2 {
            if self.revoke {
                self.target.keep(&[]).unwrap();
            } else {
                let actor = store::Actor::new(&actor()).unwrap();
                self.source.truncate(&actor, 2).unwrap();
            }
        }
        let (records, more) = self
            .source
            .records(request.start, request.end, &request.after, 2)
            .unwrap();
        Ok(pb::RepairReply {
            records,
            more,
            repaired: Vec::new(),
        })
    }
}

fn source() -> Arc<Store> {
    let store = Store::new(Arc::new(MemoryEngine::new("volume")));
    let actor = store::Actor::new(&actor()).unwrap();
    store.claim(&actor, &holder(), 1, 0, |_| false).unwrap();
    let entries = (1..=3)
        .map(|seq| pb::Entry {
            seq,
            epoch: 1,
            changes: vec![pb::Change {
                key: format!("k{seq}").into_bytes(),
                value: Some(vec![1]),
            }],
            ..Default::default()
        })
        .collect();
    store
        .append(
            &actor,
            &pb::AppendRequest {
                holder: Some(holder()),
                epoch: 1,
                entries,
                commit: 3,
                ..Default::default()
            },
        )
        .unwrap();
    Arc::new(store)
}

fn handoff() -> Directory {
    Directory::from([(
        0,
        vec![
            Owner {
                node: "c".into(),
                state: State::Leaving,
            },
            Owner {
                node: "d".into(),
                state: State::Populating,
            },
        ],
    )])
}

async fn copied(revoke: bool) -> (Arc<Store>, bool) {
    let target = Arc::new(Store::new(Arc::new(MemoryEngine::new("volume"))));
    decide(&target, &Directory::new(), "d").unwrap();
    let peers = Arc::new(Leaving {
        source: source(),
        target: target.clone(),
        pages: Mutex::new(0),
        revoke,
    });
    let held = Repair::new(target.clone(), peers, "d", "cell-c")
        .tick(&handoff(), &BTreeMap::new())
        .await
        .unwrap();
    (target, held)
}

#[tokio::test]
async fn an_actor_truncated_between_pages_is_read_again_whole() {
    let (target, held) = copied(false).await;
    assert!(held);
    let actor = store::Actor::new(&actor()).unwrap();
    let (answer, entries, _) = target.log(&actor, &holder(), 1, 0, 10).unwrap();
    let slot = answer.slot.unwrap();
    assert_eq!((slot.flushed, slot.flushed_epoch), (2, 1));
    assert_eq!(entries.iter().map(|e| e.seq).collect::<Vec<_>>(), [3]);
}

#[tokio::test]
async fn a_copy_whose_range_a_pushed_set_dropped_is_not_held() {
    let (target, held) = copied(true).await;
    assert!(!held);
    assert!(!target.held().overlaps(0, 0));
}

#[tokio::test]
async fn a_copy_made_for_another_assignment_is_made_again() {
    let target = Arc::new(Store::new(Arc::new(MemoryEngine::new("volume"))));
    decide(&target, &Directory::new(), "d").unwrap();
    // A copy from an earlier assignment of the range, at generation 1.
    target.hold(0, 0, 1).unwrap();
    let peers = Arc::new(Leaving {
        source: source(),
        target: target.clone(),
        pages: Mutex::new(5),
        revoke: false,
    });
    let repair = Repair::new(target.clone(), peers, "d", "cell-c");
    let generations = BTreeMap::from([(0, BTreeMap::from([("d".to_owned(), 2)]))]);
    assert!(repair.tick(&handoff(), &generations).await.unwrap());
    assert_eq!(target.held().generation(0, 0), Some(2));
    let actor = store::Actor::new(&actor()).unwrap();
    let (_, entries, _) = target.log(&actor, &holder(), 1, 0, 10).unwrap();
    assert_eq!(entries.len(), 3, "the copy is the leaving owner's");
    // The same assignment's copy is not made again.
    assert!(!repair.tick(&handoff(), &generations).await.unwrap());
}

#[test]
fn a_log_rewritten_at_the_same_seqs_is_torn() {
    let (records, _) = source().records(0, 0, &[], store::MAX_LIMIT).unwrap();
    assert!(consistent(&records).unwrap());
    // Another holder replaced the last entry after the claim's page: its seqs still run to the claim's end.
    let mut torn = records.clone();
    let last = torn
        .iter_mut()
        .rfind(|r| r.key.ends_with(&3u64.to_be_bytes()))
        .unwrap();
    let mut entry = pb::Entry::decode(last.value.as_slice()).unwrap();
    entry.epoch = 2;
    last.value = entry.encode_to_vec();
    assert!(!consistent(&torn).unwrap());
}

/// No other slot holds anything: a repair copies nothing and leaves the range repaired.
struct Nobody;

#[async_trait]
impl Peers for Nobody {
    async fn sources(&self, _: u64, _: u64) -> Result<Vec<Source>, Status> {
        Ok(Vec::new())
    }

    async fn slots(&self, _: &pb::Actor) -> Result<[String; actors::SLOTS], Status> {
        unreachable!("nothing is copied")
    }

    async fn page(&self, _: &Source, _: pb::RepairRequest) -> Result<pb::RepairReply, Status> {
        unreachable!("there are no sources")
    }
}

#[tokio::test]
async fn a_fresh_store_given_a_range_again_repairs_its_old_copy() {
    // A cell's first owner holds its range at generation 1, with an actor in it.
    let target = source();
    decide(&target, &Directory::new(), "d").unwrap();
    target.hold(0, 0, 1).unwrap();
    let ready = Directory::from([(
        0,
        vec![Owner {
            node: "d".into(),
            state: State::Ready,
        }],
    )]);
    let generations = BTreeMap::from([(0, BTreeMap::from([("d".to_owned(), 2)]))]);
    let held = Repair::new(target.clone(), Arc::new(Nobody), "d", "cell-c")
        .tick(&ready, &generations)
        .await
        .unwrap();
    assert!(held);
    assert_eq!(target.held().generation(0, 0), Some(2));
    assert!(
        target.repaired().overlaps(0, 0),
        "the range is repaired, not trusted"
    );
    let (records, _) = target.records(0, 0, &[], store::MAX_LIMIT).unwrap();
    assert!(records.is_empty(), "the stale copy is gone");
}

#[tokio::test]
async fn a_revoked_copy_leaves_the_store_unfresh_and_a_later_ready_range_repairs() {
    // A fresh store's handoff copy is revoked partway: part of the leaving owner's actor is left behind, unheld.
    let (target, held) = copied(true).await;
    assert!(!held);
    assert_eq!(target.fresh(), Some(false));
    let ready = Directory::from([(
        0,
        vec![Owner {
            node: "d".into(),
            state: State::Ready,
        }],
    )]);
    let generations = BTreeMap::from([(0, BTreeMap::from([("d".to_owned(), 2)]))]);
    let held = Repair::new(target.clone(), Arc::new(Nobody), "d", "cell-c")
        .tick(&ready, &generations)
        .await
        .unwrap();
    assert!(held);
    assert!(
        target.repaired().overlaps(0, 0),
        "the range is repaired, not trusted"
    );
    let (records, _) = target.records(0, 0, &[], store::MAX_LIMIT).unwrap();
    assert!(records.is_empty(), "the partial copy is gone");
}

#[tokio::test]
async fn a_cell_s_first_owner_holds_its_range_without_a_repair() {
    let target = Arc::new(Store::new(Arc::new(MemoryEngine::new("volume"))));
    decide(&target, &Directory::new(), "d").unwrap();
    let ready = Directory::from([(
        0,
        vec![Owner {
            node: "d".into(),
            state: State::Ready,
        }],
    )]);
    let generations = BTreeMap::from([(0, BTreeMap::from([("d".to_owned(), 1)]))]);
    let held = Repair::new(target.clone(), Arc::new(Nobody), "d", "cell-c")
        .tick(&ready, &generations)
        .await
        .unwrap();
    assert!(held);
    assert_eq!(target.fresh(), Some(true));
    assert!(!target.repaired().overlaps(0, 0));
}

#[tokio::test]
async fn records_of_a_range_left_without_a_mark_are_cleared_once_another_owner_is_ready() {
    // A pushed set forgot the hold before the revoke was seen: the records stay, with no held or sealed mark.
    let target = source();
    decide(&target, &Directory::new(), "d").unwrap();
    let elsewhere = Directory::from([(
        0,
        vec![Owner {
            node: "e".into(),
            state: State::Ready,
        }],
    )]);
    Repair::new(target.clone(), Arc::new(Nobody), "d", "cell-c")
        .tick(&elsewhere, &BTreeMap::new())
        .await
        .unwrap();
    let (records, _) = target.records(0, 0, &[], store::MAX_LIMIT).unwrap();
    assert!(records.is_empty());
}

#[test]
fn a_log_record_whose_key_names_another_entry_is_corruption() {
    let (mut records, _) = source().records(0, 0, &[], store::MAX_LIMIT).unwrap();
    let last = records
        .iter_mut()
        .rfind(|r| r.key.ends_with(&3u64.to_be_bytes()))
        .unwrap();
    let mut entry = pb::Entry::decode(last.value.as_slice()).unwrap();
    entry.seq = 4;
    last.value = entry.encode_to_vec();
    assert_eq!(
        consistent(&records).unwrap_err().code(),
        tonic::Code::DataLoss
    );
}
