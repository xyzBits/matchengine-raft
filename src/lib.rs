//! openraft + actix-web 实现的分布式 Key-Value 存储示例
//! 可以处理 http 请求，并保持多节点数据一致性的服务
use std::sync::Arc;
use std::time::Duration;

use actix_web::middleware;
use actix_web::middleware::Logger;
use actix_web::web::Data;
use actix_web::App;
use actix_web::HttpServer;
use openraft::Config;
use openraft::Raft;
use openraft::SnapshotPolicy;

use crate::app::ExampleApp;
use crate::network::api;
use crate::network::management;
use crate::network::raft;
use crate::network::raft_network_impl::ExampleNetwork;
use crate::store::ExampleRequest;
use crate::store::ExampleResponse;
use crate::store::ExampleStore;
use crate::store::Restore;

pub mod app;
pub mod client;
pub mod matchengine;
pub mod network;
pub mod store;

// 核心类型定义，
// 代码首先定义了 raft 系统中使用的具体数据格式，
// raft 算法是通用的，但每个应用需要定义自己的 请求 和 响应 格式
// 节点的唯一标识符
pub type ExampleNodeId = u64;

// 将业务逻辑（ExampleRequest 和 ExampleResponse）与 Raft 引擎绑定
// 这意味着 raft 集群是专门用来处理你所定义的这些请求和响应的
// 通过这个宏，生成了一个实现了RaftTypeConfig trait 的 ExampleTypeConfig 结构体
openraft::declare_raft_types!(
    /// Declare the type configuration for example K/V store.
    /// ExampleRequest 是客户端发送的请求类型，包含了 set/get 操作的数据，
    /// 不包含 raft 节点通信的请求，这是由库底层做的，并且是一直不变的
    /// 而 request 是经常发生变化的，需要用户自己来定义
    pub ExampleTypeConfig: D = ExampleRequest, R = ExampleResponse, NodeId = ExampleNodeId
);

// 类型别名，代表了这个具体的 Raft 实例，它组合了 配置 ExampleTypeConfig、网络实现 ExampleNetwork 和 存储实现 ExampleStore
pub type ExampleRaft = Raft<ExampleTypeConfig, ExampleNetwork, Arc<ExampleStore>>;

/// 程序的主逻辑入口，负责初始化所有组件并 启动 web 服务器
/// 这段代码是一个典型的 Shared-Nothing 架构 的分布式节点启动模版：
/// 定义协议：告诉 OpenRaft 数据长什么样。
/// 准备环境：配置参数、打开数据库、准备网络发送器。
/// 启动核心：运行 Raft 算法后台。
///对外服务：通过 HTTP 接口，既处理客户端的读写，也处理其他节点的投票和复制请求。
pub async fn start_example_raft_node(
    node_id: ExampleNodeId,
    http_addr: String,
) -> std::io::Result<()> {
    // Create a configuration for the raft instance.

    // 配置 Raft 实例的行为参数，防止日志无限增长
    let mut config = Config::default().validate().unwrap();
    config.snapshot_policy = SnapshotPolicy::LogsSinceLast(500); // 每隔 500 条日志生成一个快照
    config.max_applied_log_to_keep = 20000; // 保留最近 20000 条已应用的日志
    config.install_snapshot_timeout = 400; // 快照安装超时时间设置为 400 秒

    let config = Arc::new(config);

    // Create a instance of where the Raft data will be stored.
    // 初始化存储层(raft的持久化层，负责保存日志log、状态机 state machine、和当前raft状态)
    let es = ExampleStore::open_create(node_id);

    //es.load_latest_snapshot().await.unwrap();

    let mut store = Arc::new(es);

    // 节点重启时，从 disk 重放日志或者加载快照来恢复状态
    store.restore().await;

    // Create the network layer that will connect and communicate the raft instances and
    // will be used in conjunction with the store created above.
    // 初始化网络层(节点之间的通信，leader发送心跳给followr)，openraft 不包含网络实现，这里使用自定义的 example network，通常基于 http 或 rpc 实现
    let network = ExampleNetwork::new();

    // Create a local raft instance.
    // 创建 raft 实现，new 启动了后台任务，开始运行共识算法（选举、日志复制等）
    let raft = Raft::new(node_id, config.clone(), network, store.clone());

    // Create an application that will store all the instances created above, this will
    // be later used on the actix-web services.
    // 构建应用状态
    // 将 raft 实例， store和配置打包进 ExampleApp, 并使用 Data 在 actix-web 中共享，
    // 这样在 http 处理函数中可以通过依赖注入访问到 raft 核心，从而提交数据或者查询 状态
    let app = Data::new(ExampleApp {
        id: node_id,
        addr: http_addr.clone(),
        raft,
        store,
        config,
    });

    // Start the actix-web server.
    // 启动 http 服务器，暴露了三类 api 接口
    let server = HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .wrap(Logger::new("%a %{User-Agent}i"))
            .wrap(middleware::Compress::default())
            .app_data(app.clone())
            // raft internal RPC
            // 1. raft 内部通信 API (节点调用)
            .service(raft::append) // 用于日志复制
            .service(raft::snapshot) // 用于发送快照
            .service(raft::vote) // 用于选举投票
            // admin API
            // 2. 管理 API (集群管理，如添加节点、变更成员等)，运维人员调用
            .service(management::init) // 初始化集群
            .service(management::add_learner) // 添加 learner 节点
            .service(management::change_membership) // 变更集群成员
            .service(management::metrics) // 获取节点监控指标
            // application API
            // 3. 应用 API (客户端调用)，对外提供的读写接口
            .service(api::write) // 写请求，会转发到 leader 节点
            .service(api::read) // 读请求
            .service(api::consistent_read) // 一致性读，确保读到最新数据
    })
    .keep_alive(Duration::from_secs(5));

    let x = server.bind(http_addr)?;

    x.run().await
}
