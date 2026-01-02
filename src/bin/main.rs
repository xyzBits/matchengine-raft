use clap::Parser;
use env_logger::Env;
use example_raft_key_value::network::raft_network_impl::ExampleNetwork;
use example_raft_key_value::start_example_raft_node;
use example_raft_key_value::store::ExampleStore;
use example_raft_key_value::ExampleTypeConfig;
use openraft::Raft;

// 定义一个类型别名
// openraft 的核心用法，
// ExampleTypeConfig 定义了 Raft 节点的配置类型
// ExampleNetwork 定义了 Raft 节点之间的网络通信实现，定义节点之间如何通信
// ExampleStore 定义了 Raft 节点的数据存储实现，数据如何持久化
pub type ExampleRaft = Raft<ExampleTypeConfig, ExampleNetwork, ExampleStore>;

#[derive(Parser, Clone, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Opt {
    // Raft 节点的唯一标识符
    #[clap(long)]
    pub id: u64,

    // Raft 节点监听的 HTTP 地址，用于接收客户端请求（如set get）以及节点之间的 raft 消息（比如投票、追加日志）
    #[clap(long)]
    pub http_addr: String,
}

// 里面包含 tokio::main 宏，并且启动了 actix-web 所需要的很多组件
#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Setup the logger
    // 1。初始化日志，打印 info 级别及以上的日志
    env_logger::init_from_env(Env::default().default_filter_or("info"));

    // Parse the parameters passed by arguments.
    // 2。解析命令行参数
    let options = Opt::parse();

    // 3。启动节点
    start_example_raft_node(options.id, options.http_addr).await
}
