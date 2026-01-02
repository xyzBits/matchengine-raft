use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;
use std::sync::Mutex;

use openraft::async_trait::async_trait;
use openraft::storage::LogState;
use openraft::storage::Snapshot;
use openraft::AnyError;
use openraft::EffectiveMembership;
use openraft::Entry;
use openraft::EntryPayload;
use openraft::ErrorSubject;
use openraft::ErrorVerb;
use openraft::LogId;
use openraft::RaftLogReader;
use openraft::RaftSnapshotBuilder;
use openraft::RaftStorage;
use openraft::SnapshotMeta;
use openraft::StateMachineChanges;
use openraft::StorageError;
use openraft::StorageIOError;
use openraft::Vote;
use serde::Deserialize;
use serde::Serialize;
use sled::{Db, IVec};
use tokio::sync::RwLock;

use crate::matchengine::Order;
use crate::matchengine::OrderBook;
use crate::ExampleNodeId;
use crate::ExampleTypeConfig;
pub mod config;
pub mod store;

use crate::store::config::Config;

#[derive(Debug)]
pub struct ExampleSnapshot {
    pub meta: SnapshotMeta<ExampleNodeId>,

    /// The data of the state machine at the time of this snapshot.
    pub data: Vec<u8>,
}

/**
 * Here you will set the types of request that will interact with the raft nodes.
 * For example the `Set` will be used to write data (key and value) to the raft database.
 * The `AddNode` will append a new node to the current existing shared list of nodes.
 * You will want to add any request that can write data in all nodes here.
 */
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ExampleRequest {
    Set { key: String, value: String },
    Place { order: Order },
    Cancel { order: Order },
}

/**
 * Here you will defined what type of answer you expect from reading the data of a node.
 * In this example it will return a optional value from a given key in
 * the `ExampleRequest.Set`.
 *
 * TODO: SHould we explain how to create multiple `AppDataResponse`?
 *
 */
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ExampleResponse {
    pub value: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct StateMachineContent {
    pub last_applied_log: Option<LogId<ExampleNodeId>>,

    // TODO: it should not be Option.
    pub last_membership: EffectiveMembership<ExampleNodeId>,

    /// Application data.
    pub data: BTreeMap<String, String>,

    // ** Orderbook
    pub orders: Vec<Order>,

    pub sequance: u64,
}

/**
 * Here defines a state machine of the raft, this state represents a copy of the data
 * between each node. Note that we are using `serde` to serialize the `data`, which has
 * a implementation to be serialized. Note that for this test we set both the key and
 * value as String, but you could set any type of value that has the serialization impl.
 */
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct ExampleStateMachine {
    pub last_applied_log: Option<LogId<ExampleNodeId>>,

    // TODO: it should not be Option.
    pub last_membership: EffectiveMembership<ExampleNodeId>,

    /// Application data.
    pub data: BTreeMap<String, String>,

    // ** Orderbook
    pub orderbook: OrderBook,
}

impl ExampleStateMachine {
    pub fn to_content(&self) -> StateMachineContent {
        let mut content = StateMachineContent {
            last_applied_log: self.last_applied_log,
            last_membership: self.last_membership.clone(),
            data: self.data.clone(),
            orders: vec![],
            sequance: self.orderbook.sequance,
        };

        let mut bids: Vec<Order> = self.orderbook.bids.values().cloned().collect();
        let mut asks: Vec<Order> = self.orderbook.asks.values().cloned().collect();

        content.orders.append(&mut bids);
        content.orders.append(&mut asks);
        return content;
    }

    pub fn from_content(&mut self, content: &StateMachineContent) {
        // ** Build from content **
        self.last_applied_log = content.last_applied_log;
        self.last_membership = content.last_membership.clone();
        self.data = content.data.clone();
        self.orderbook.asks.clear();
        self.orderbook.bids.clear();
        self.orderbook.sequance = content.sequance;

        for order in content.orders.clone() {
            self.orderbook.insert_order(&order);
        }
    }
}

#[derive(Debug)]
pub struct ExampleStore {
    // 日志清理标记
    // 记录了最后一次被清理 compaction 的日志id
    // 当生成快照后，旧日志会被删除 ，这里记录删到了哪里
    last_purged_log_id: RwLock<Option<LogId<ExampleNodeId>>>,

    /// The Raft log.
    /// raft 日志，持久化核心
    /// 使用 sled 作为底层存储引擎
    /// 这里存储了所有的操作指令 entry
    /// 必须持久化，否则重启后数据丢失，会导致一致性错误
    pub log: sled::Tree, //RwLock<BTreeMap<u64, Entry<StorageRaftTypeConfig>>>,

    /// The Raft state machine.
    /// 状态机（业务数据）
    /// 这里存放着实际的应用状态，比如你的 key-value 数据
    /// 是用 RwLock 包裹的内存结构，允许并发读写
    /// 因为是内存结构，意味着重启时需要从 log 或者 snapshot 恢复
    pub state_machine: RwLock<ExampleStateMachine>,

    /// The current granted vote.
    /// 投票记录，硬状态
    /// 记录我当前的任期投票给了谁
    /// 协议要求，一个节点在一个任期内只能投一票
    /// 如果不持久化，节点重启后可能重复投票，导致选出两个 leader
    vote: sled::Tree,

    // 快照相关，
    // 用于管理和传输快照的数据结构
    snapshot_idx: Arc<Mutex<u64>>,

    current_snapshot: RwLock<Option<ExampleSnapshot>>,

    // 配置与节点
    config: Config,

    pub node_id: ExampleNodeId,
}

/// 连接 磁盘
/// 负责根据配置和节点 id，找到对应的磁盘路径并打开数据库文件
fn get_sled_db(config: Config, node_id: ExampleNodeId) -> Db {
    let db_path = format!(
        "{}/{}-{}.binlog",
        config.journal_path, config.instance_prefix, node_id
    );

    // 打开 sled 数据库，如果文件不存在，会自动创建
    let db = sled::open(db_path.clone()).unwrap();
    tracing::debug!("get_sled_db: created log at: {:?}", db_path);
    db
}

impl ExampleStore {
    /// 初始化函数，建立逻辑分区
    pub fn open_create(node_id: ExampleNodeId) -> ExampleStore {
        tracing::info!("open_create, node_id: {}", node_id);

        let config = Config::default();

        // 数据库连接
        let db = get_sled_db(config.clone(), node_id);

        // 创建/打开 journal_entities_{id} 的 tree
        // 专门用来存放 raft 日志，相当于日志表 或者  bucket
        let log = db
            .open_tree(format!("journal_entities_{}", node_id))
            .unwrap();

        // 打开/创建 名为 votes_{id} 的 tree
        // 专门用来存投票信息 vote，相当于日志表
        let vote = db.open_tree(format!("votes_{}", node_id)).unwrap();

        let current_snapshot = RwLock::new(None);

        ExampleStore {
            last_purged_log_id: Default::default(),
            config: config,
            node_id: node_id,
            log,                               // 日志表(树)
            state_machine: Default::default(), // 状态机初始化为 空，后续通过 replay 恢复
            vote: vote,                        // 投票表(树)
            snapshot_idx: Arc::new(Mutex::new(0)),
            current_snapshot,
        }
    }
}

/// Store trait for restore things from snapshot and log
/// 节点重启时，从磁盘 sled 数据库 中恢复 节点的内存状态
/// 节点崩溃或者重启后，内存里的数据，如状态机、当前的配置都会丢失，这个函数负责把 磁盘上的硬状态重新加载回软状态 内存
/// async_trait 可以让我们在 trait 中定义异步函数  async fn
#[async_trait]
pub trait Restore {
    async fn restore(&mut self);
}

#[async_trait]
impl Restore for Arc<ExampleStore> {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn restore(&mut self) {
        tracing::debug!("restore");

        // 获取日志树
        let log = &self.log;

        let first = log
            .iter() // 创建迭代器
            .rev() // 反转迭代器，从大到小
            .next() // 取第一个元素，也即原序列的最后一个，即最新的日志
            .map(|res| res.unwrap())
            .map(|(_, val)| {
                // 反序列化二进制数据为 entry 结构体
                serde_json::from_slice::<Entry<ExampleTypeConfig>>(&*val)
                    .unwrap()
                    .log_id // 提取 logId
            });

        match first {
            Some(x) => {
                tracing::debug!("restore: first log id = {:?}", x);

                // 如果存在，更新内存中的 last_purged_log_id
                let mut ld = self.last_purged_log_id.write().await;
                *ld = Some(x);
            }
            None => {}
        }

        // 检查磁盘上有没有生成的快照文件
        let snapshot = self.get_current_snapshot().await.unwrap();

        match snapshot {
            Some(ss) => {
                // 解析快照数据，并将节点状态机的数据重置为快照
                self.install_snapshot(&ss.meta, ss.snapshot).await.unwrap();
            }
            None => {}
        }
    }
}

#[async_trait]
impl RaftLogReader<ExampleTypeConfig> for Arc<ExampleStore> {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_log_state(
        &mut self,
    ) -> Result<LogState<ExampleTypeConfig>, StorageError<ExampleNodeId>> {
        let log = &self.log;
        let last = log
            .iter()
            .rev()
            .next()
            .map(|res| res.unwrap())
            .map(|(_, val)| {
                serde_json::from_slice::<Entry<ExampleTypeConfig>>(&*val)
                    .unwrap()
                    .log_id
            });

        let last_purged = *self.last_purged_log_id.read().await;

        let last = match last {
            None => last_purged,
            Some(x) => Some(x),
        };
        tracing::debug!(
            "get_log_state: last_purged = {:?}, last = {:?}",
            last_purged,
            last
        );
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last,
        })
    }

    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send + Sync>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<ExampleTypeConfig>>, StorageError<ExampleNodeId>> {
        let log = &self.log;
        let response = log
            .range(transform_range_bound(range))
            .map(|res| res.unwrap())
            .map(|(_, val)| serde_json::from_slice::<Entry<ExampleTypeConfig>>(&*val).unwrap())
            .collect();

        Ok(response)
    }
}

fn transform_range_bound<RB: RangeBounds<u64> + Clone + Debug + Send + Sync>(
    range: RB,
) -> (Bound<IVec>, Bound<IVec>) {
    (
        serialize_bound(&range.start_bound()),
        serialize_bound(&range.end_bound()),
    )
}

fn serialize_bound(v: &Bound<&u64>) -> Bound<IVec> {
    match v {
        Bound::Included(v) => Bound::Included(IVec::from(&v.to_be_bytes())),
        Bound::Excluded(v) => Bound::Excluded(IVec::from(&v.to_be_bytes())),
        Bound::Unbounded => Bound::Unbounded,
    }
}

#[async_trait]
impl RaftSnapshotBuilder<ExampleTypeConfig, Cursor<Vec<u8>>> for Arc<ExampleStore> {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<ExampleTypeConfig, Cursor<Vec<u8>>>, StorageError<ExampleNodeId>> {
        let (data, last_applied_log);

        {
            // Serialize the data of the state machine.
            let state_machine = self.state_machine.read().await;
            data = serde_json::to_vec(&*state_machine).map_err(|e| {
                StorageIOError::new(
                    ErrorSubject::StateMachine,
                    ErrorVerb::Read,
                    AnyError::new(&e),
                )
            })?;

            last_applied_log = state_machine.last_applied_log;
        }

        let last_applied_log = match last_applied_log {
            None => {
                panic!("can not compact empty state machine");
            }
            Some(x) => x,
        };

        let snapshot_idx = {
            let mut l = self.snapshot_idx.lock().unwrap();
            *l += 1;
            *l
        };

        let snapshot_id = format!(
            "{}-{}-{}",
            last_applied_log.leader_id, last_applied_log.index, snapshot_idx
        );

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            snapshot_id,
        };

        let snapshot = ExampleSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };

        {
            let mut current_snapshot = self.current_snapshot.write().await;
            *current_snapshot = Some(snapshot);
        }

        self.write_snapshot().await.unwrap();

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

#[async_trait]
impl RaftStorage<ExampleTypeConfig> for Arc<ExampleStore> {
    type SnapshotData = Cursor<Vec<u8>>;
    type LogReader = Self;
    type SnapshotBuilder = Self;

    #[tracing::instrument(level = "trace", skip(self))]
    async fn save_vote(
        &mut self,
        vote: &Vote<ExampleNodeId>,
    ) -> Result<(), StorageError<ExampleNodeId>> {
        self.vote
            .insert(b"vote", IVec::from(serde_json::to_vec(vote).unwrap()))
            .unwrap();
        Ok(())
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<Vote<ExampleNodeId>>, StorageError<ExampleNodeId>> {
        let value = self.vote.get(b"vote").unwrap();
        match value {
            None => Ok(None),
            Some(val) => Ok(Some(
                serde_json::from_slice::<Vote<ExampleNodeId>>(&*val).unwrap(),
            )),
        }
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn append_to_log(
        &mut self,
        entries: &[&Entry<ExampleTypeConfig>],
    ) -> Result<(), StorageError<ExampleNodeId>> {
        let log = &self.log;
        for entry in entries {
            log.insert(
                entry.log_id.index.to_be_bytes(),
                IVec::from(serde_json::to_vec(&*entry).unwrap()),
            )
            .unwrap();
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<ExampleNodeId>,
    ) -> Result<(), StorageError<ExampleNodeId>> {
        tracing::debug!("delete_log: [{:?}, +oo)", log_id);

        let log = &self.log;
        let keys = log
            .range(transform_range_bound(log_id.index..))
            .map(|res| res.unwrap())
            .map(|(k, _v)| k); //TODO Why originally used collect instead of the iter.
        for key in keys {
            log.remove(&key).unwrap();
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn purge_logs_upto(
        &mut self,
        log_id: LogId<ExampleNodeId>,
    ) -> Result<(), StorageError<ExampleNodeId>> {
        tracing::debug!("delete_log: [{:?}, +oo)", log_id);

        {
            let mut ld = self.last_purged_log_id.write().await;
            assert!(*ld <= Some(log_id));
            *ld = Some(log_id);
        }

        {
            let log = &self.log;

            let keys = log
                .range(transform_range_bound(..=log_id.index))
                .map(|res| res.unwrap())
                .map(|(k, _)| k);
            for key in keys {
                log.remove(&key).unwrap();
            }
        }

        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<ExampleNodeId>>,
            EffectiveMembership<ExampleNodeId>,
        ),
        StorageError<ExampleNodeId>,
    > {
        let state_machine = self.state_machine.read().await;
        Ok((
            state_machine.last_applied_log,
            state_machine.last_membership.clone(),
        ))
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn apply_to_state_machine(
        &mut self,
        entries: &[&Entry<ExampleTypeConfig>],
    ) -> Result<Vec<ExampleResponse>, StorageError<ExampleNodeId>> {
        let mut res = Vec::with_capacity(entries.len());

        let mut sm = self.state_machine.write().await;

        for entry in entries {
            tracing::debug!(%entry.log_id, "replicate to sm");

            sm.last_applied_log = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => res.push(ExampleResponse { value: None }),
                EntryPayload::Normal(ref req) => match req {
                    ExampleRequest::Set { key, value } => {
                        sm.data.insert(key.clone(), value.clone());
                        res.push(ExampleResponse {
                            value: Some(value.clone()),
                        })
                    }
                    ExampleRequest::Place { order } => {
                        let mut o = order.clone();
                        let mr = sm.orderbook.place_order(&mut o);
                        res.push(ExampleResponse {
                            value: Some(o.sequance.to_string()),
                        })
                    }

                    ExampleRequest::Cancel { order } => {
                        sm.orderbook.cancle(order);
                        res.push(ExampleResponse {
                            value: Some("OK".to_string()),
                        })
                    }
                },
                EntryPayload::Membership(ref mem) => {
                    sm.last_membership = EffectiveMembership::new(Some(entry.log_id), mem.clone());
                    res.push(ExampleResponse { value: None })
                }
            };
        }
        Ok(res)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Self::SnapshotData>, StorageError<ExampleNodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    #[tracing::instrument(level = "trace", skip(self, snapshot))]
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<ExampleNodeId>,
        snapshot: Box<Self::SnapshotData>,
    ) -> Result<StateMachineChanges<ExampleTypeConfig>, StorageError<ExampleNodeId>> {
        tracing::info!(
            { snapshot_size = snapshot.get_ref().len() },
            "decoding snapshot for installation"
        );

        let new_snapshot = ExampleSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };

        // Update the state machine.
        {
            let updated_state_machine: ExampleStateMachine =
                serde_json::from_slice(&new_snapshot.data).map_err(|e| {
                    StorageIOError::new(
                        ErrorSubject::Snapshot(new_snapshot.meta.clone()),
                        ErrorVerb::Read,
                        AnyError::new(&e),
                    )
                })?;
            let mut state_machine = self.state_machine.write().await;
            *state_machine = updated_state_machine;
        }

        // Update current snapshot.
        let mut current_snapshot = self.current_snapshot.write().await;
        *current_snapshot = Some(new_snapshot);
        Ok(StateMachineChanges {
            last_applied: meta.last_log_id,
            is_snapshot: true,
        })
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<ExampleTypeConfig, Self::SnapshotData>>, StorageError<ExampleNodeId>>
    {
        tracing::debug!("get_current_snapshot: start");

        match &*self.current_snapshot.read().await {
            Some(snapshot) => {
                let data = snapshot.data.clone();
                Ok(Some(Snapshot {
                    meta: snapshot.meta.clone(),
                    snapshot: Box::new(Cursor::new(data)),
                }))
            }
            None => {
                let data = self.read_snapshot_file().await;
                //tracing::debug!("get_current_snapshot: data = {:?}",data);

                let data = match data {
                    Ok(c) => c,
                    Err(_e) => return Ok(None),
                };

                let content: ExampleStateMachine = serde_json::from_slice(&data).unwrap();

                let last_applied_log = content.last_applied_log.unwrap();
                tracing::debug!(
                    "get_current_snapshot: last_applied_log = {:?}",
                    last_applied_log
                );

                let snapshot_idx = {
                    let mut l = self.snapshot_idx.lock().unwrap();
                    *l += 1;
                    *l
                };

                let snapshot_id = format!(
                    "{}-{}-{}",
                    last_applied_log.leader_id, last_applied_log.index, snapshot_idx
                );

                let meta = SnapshotMeta {
                    last_log_id: last_applied_log,
                    snapshot_id: snapshot_id,
                };

                tracing::debug!("get_current_snapshot: meta {:?}", meta);

                Ok(Some(Snapshot {
                    meta: meta,
                    snapshot: Box::new(Cursor::new(data)),
                }))
            }
        }
    }
}
