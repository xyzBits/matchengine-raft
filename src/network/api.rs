use actix_web::post;
use actix_web::web;
use actix_web::web::Data;
use actix_web::Responder;
use openraft::error::CheckIsLeaderError;
use openraft::error::Infallible;
use openraft::raft::ClientWriteRequest;
use openraft::EntryPayload;
use web::Json;

use crate::app::ExampleApp;
use crate::store::ExampleRequest;
use crate::ExampleNodeId;

/**
 * Application API
 *
 * This is where you place your application, you can use the example below to create your
 * API. The current implementation:
 *
 *  - `POST - /write` saves a value in a key and sync the nodes.
 *  - `POST - /read` attempt to find a value from a given key.
 */

/// 1。leader 检查
///        如果是 leader，会把请求生成一条日志 log entry，存入本地磁盘，通过网络发送给所有 follower
///        如果是 follower ，会拒绝请求，并告诉client，谁是leader
/// 2。达成共识 quorum
///        leader 等待，直到大数(超过半数)节点都回身 收到并写入日志成功
/// 3。应用状态机 apply
///        一旦达成共识，leader 将这条日志应用到自己的状态机，也就是执行 set 操作，更新内存 map
/// 4。返回结果
///        client_write 返回执行结果，
#[post("/write")]
pub async fn write(
    app: Data<ExampleApp>,     // 全局应用状态
    req: Json<ExampleRequest>, //解析请求体
) -> actix_web::Result<impl Responder> {
    let request = ClientWriteRequest::new(EntryPayload::Normal(req.0));
    let response = app.raft.client_write(request).await;
    Ok(Json(response))
}

/// 直接从当前节点的内存中读取数据并返回给客户端
/// store.state_machine.read() 不检查当前节点是不是 leader，也不检查当前节点的数据是不是最新的
/// 速度极快，只需要获取内存锁，没有任何网络 io 性能是最高的
/// 风险
///     数据陈旧：如果这是一个与世隔绝的 follower，它可能落后 leader 1000 条日志，你调用这个接口，读到的是旧数据
/// 
#[post("/read")]
pub async fn read(app: Data<ExampleApp>, req: Json<String>) -> actix_web::Result<impl Responder> {
    // 获取状态机的读锁，
    // 这里没有直接调用 app.raft，而是直接调用 app.store
    // read.await是 RwLock 的读锁，支持多线程并发读取
    let state_machine = app.store.state_machine.read().await;
    let key = req.0;

    // 根据 key 进行模式匹配，返回不同的业务数据
    let value = match key.as_str() {
        "orderbook_orders" => {
            serde_json::to_string(&state_machine.to_content().orders).unwrap_or_default()
        }
        "orderbook_sequance" => state_machine.orderbook.sequance.to_string(),
        _ => state_machine.data.get(&key).cloned().unwrap_or_default(),
    };
    let res: Result<String, Infallible> = Ok(value);
    Ok(Json(res))
}


/// 一致性读，或者更准确的说是 强制leader 读
/// 核心目的是，确保这条读请求一定是由集群当前的 leader 处理的，从而避免读到 follower 上的旧数据  
#[post("/consistent_read")]
pub async fn consistent_read(
    app: Data<ExampleApp>,
    req: Json<String>,
) -> actix_web::Result<impl Responder> {

    // 调用 raft 核心的接口检查当前节点的身份
    let ret = app.raft.is_leader().await;

    match ret {
        // 如果是 leader
        Ok(_) => {

            // 获取本地读锁，和普通  reade 一样
            let state_machine = app.store.state_machine.read().await;
            let key = req.0;
            let value = state_machine.data.get(&key).cloned();

            let res: Result<String, CheckIsLeaderError<ExampleNodeId>> =
                Ok(value.unwrap_or_default());
            Ok(Json(res))
        }
        // 不是 leader ，将错误返回给客户端 
        Err(e) => Ok(Json(Err(e))),
    }
}
