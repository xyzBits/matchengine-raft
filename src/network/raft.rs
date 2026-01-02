//! raft 节点之间的内部通信接口
//! 为什么要在这里进行封装，
//! 因为需要把 核心逻辑(vote append-entry install-snapshot)与网络传输完全解耦
//! openraft 作为 lib，不想管你的信息是如何传递的 
//! 以及是如何序列化的，你可以用 protobuf json 或者任何你想要的一种 
use actix_web::post;
use actix_web::web;
use actix_web::web::Data;
use actix_web::Responder;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::VoteRequest;
use web::Json;

use crate::app::ExampleApp;
use crate::ExampleNodeId;
use crate::ExampleTypeConfig;

// --- Raft communication

/// 投票接口
#[post("/raft-vote")]
pub async fn vote(
    app: Data<ExampleApp>,
    req: Json<VoteRequest<ExampleNodeId>>,
) -> actix_web::Result<impl Responder> {
    let res = app.raft.vote(req.0).await;
    Ok(Json(res))
}

/// 日志复制 心跳接口 
#[post("/raft-append")]
pub async fn append(
    app: Data<ExampleApp>,
    req: Json<AppendEntriesRequest<ExampleTypeConfig>>,
) -> actix_web::Result<impl Responder> {
    let res = app.raft.append_entries(req.0).await;
    Ok(Json(res))
}


/// 安装快照接口
/// 某个 follower 宕机太久，重启后发现落后leader 太多，
/// leader 发现自己的日志已经被清理，找不到这 10 万条日志发给它
/// leader 就会把当前的快照文件(状态机的完整压缩包)通过这个接口直接发给 follower 
#[post("/raft-snapshot")]
pub async fn snapshot(
    app: Data<ExampleApp>,
    req: Json<InstallSnapshotRequest<ExampleTypeConfig>>,
) -> actix_web::Result<impl Responder> {
    let res = app.raft.install_snapshot(req.0).await;
    Ok(Json(res))
}
