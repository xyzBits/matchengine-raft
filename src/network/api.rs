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

#[post("/read")]
pub async fn read(app: Data<ExampleApp>, req: Json<String>) -> actix_web::Result<impl Responder> {
    let state_machine = app.store.state_machine.read().await;
    let key = req.0;
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

#[post("/consistent_read")]
pub async fn consistent_read(
    app: Data<ExampleApp>,
    req: Json<String>,
) -> actix_web::Result<impl Responder> {
    let ret = app.raft.is_leader().await;

    match ret {
        Ok(_) => {
            let state_machine = app.store.state_machine.read().await;
            let key = req.0;
            let value = state_machine.data.get(&key).cloned();

            let res: Result<String, CheckIsLeaderError<ExampleNodeId>> =
                Ok(value.unwrap_or_default());
            Ok(Json(res))
        }
        Err(e) => Ok(Json(Err(e))),
    }
}
