//! End-to-end gRPC tests for the workflow matching service over a real TCP
//! socket (tonic client <-> server). Exercises the full wire path: add, long-
//! poll pull, streaming pull, idempotent completion, visibility redelivery, and
//! per-task-queue isolation.

use std::sync::Arc;
use std::time::Duration;

use tpt_workflow::proto::synapse::workflow::v1::matching_service_client::MatchingServiceClient;
use tpt_workflow::proto::synapse::workflow::v1::{
    AddTaskRequest, PollTaskRequest, RespondRequest, StreamTasksRequest,
};
use tpt_workflow::{spawn, ServerHandle, TaskQueueManager};

async fn start(
) -> (SocketAddr, MatchingServiceClient<tonic::transport::Channel>, ServerHandle) {
    let core = Arc::new(synapse_core::SynapseCore::new());
    let mgr = TaskQueueManager::new(core);
    let (addr, handle) = spawn(mgr, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let client = MatchingServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    (addr, client, handle)
}

use std::net::SocketAddr;

#[tokio::test]
async fn grpc_add_poll_respond() {
    let (_addr, mut client, _handle) = start().await;

    client
        .add_task(AddTaskRequest {
            tenant: "acme".into(),
            task_queue: "activities".into(),
            task_type: "activity".into(),
            payload: b"run-this".to_vec(),
            visibility_timeout_ms: 0,
        })
        .await
        .unwrap();

    let resp = client
        .poll_task(PollTaskRequest {
            tenant: "acme".into(),
            task_queue: "activities".into(),
            worker_id: "w1".into(),
            task_type: "activity".into(),
            long_poll_timeout_ms: 2000,
        })
        .await
        .unwrap()
        .into_inner();
    let task = resp.task.expect("task pulled");
    assert_eq!(task.payload, b"run-this");
    assert_eq!(task.task_type, "activity");
    assert_eq!(task.attempt, 1);

    let r = client
        .respond_task_completed(RespondRequest {
            task_token: task.task_token.clone(),
            result_payload: b"ok".to_vec(),
            failure_message: String::new(),
            failure_stack: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(r.accepted && !r.duplicate);

    // Idempotent duplicate completion.
    let r2 = client
        .respond_task_completed(RespondRequest {
            task_token: task.task_token,
            result_payload: b"ok".to_vec(),
            failure_message: String::new(),
            failure_stack: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(r2.accepted && r2.duplicate);
}

#[tokio::test]
async fn grpc_long_poll_waits_for_task() {
    let (_addr, mut client, _handle) = start().await;

    // Schedule an AddTask 150ms into the future; the long-poll should block and
    // then return the task rather than timing out empty.
    let adder = {
        let mut c = client.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(150)).await;
            c.add_task(AddTaskRequest {
                tenant: "acme".into(),
                task_queue: "q".into(),
                task_type: String::new(),
                payload: b"late".to_vec(),
                visibility_timeout_ms: 0,
            })
            .await
            .unwrap();
        })
    };

    let start = std::time::Instant::now();
    let resp = client
        .poll_task(PollTaskRequest {
            tenant: "acme".into(),
            task_queue: "q".into(),
            worker_id: String::new(),
            task_type: String::new(),
            long_poll_timeout_ms: 2000,
        })
        .await
        .unwrap()
        .into_inner();
    let elapsed = start.elapsed();
    adder.await.unwrap();

    let task = resp.task.expect("long-poll returned the late task");
    assert_eq!(task.payload, b"late");
    assert!(elapsed >= Duration::from_millis(100), "should have waited");
}

#[tokio::test]
async fn grpc_visibility_redelivery() {
    let (_addr, mut client, _handle) = start().await;

    client
        .add_task(AddTaskRequest {
            tenant: "acme".into(),
            task_queue: "q".into(),
            task_type: String::new(),
            payload: b"retry-me".to_vec(),
            visibility_timeout_ms: 100,
        })
        .await
        .unwrap();

    let t1 = client
        .poll_task(PollTaskRequest {
            tenant: "acme".into(),
            task_queue: "q".into(),
            worker_id: String::new(),
            task_type: String::new(),
            long_poll_timeout_ms: 1000,
        })
        .await
        .unwrap()
        .into_inner()
        .task
        .unwrap();
    assert_eq!(t1.attempt, 1);

    // Never ack; after the visibility window the sweeper redelivers.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let t2 = client
        .poll_task(PollTaskRequest {
            tenant: "acme".into(),
            task_queue: "q".into(),
            worker_id: String::new(),
            task_type: String::new(),
            long_poll_timeout_ms: 1000,
        })
        .await
        .unwrap()
        .into_inner()
        .task
        .unwrap();
    assert_eq!(t2.payload, b"retry-me");
    assert_eq!(t2.attempt, 2);
}

#[tokio::test]
async fn grpc_streaming_pull() {
    let (_addr, mut client, _handle) = start().await;

    for i in 0..2u8 {
        client
            .add_task(AddTaskRequest {
                tenant: "acme".into(),
                task_queue: "stream".into(),
                task_type: String::new(),
                payload: vec![i],
                visibility_timeout_ms: 0,
            })
            .await
            .unwrap();
    }

    let mut stream = client
        .stream_tasks(StreamTasksRequest {
            tenant: "acme".into(),
            task_queue: "stream".into(),
            worker_id: String::new(),
            task_type: String::new(),
            visibility_timeout_ms: 0,
        })
        .await
        .unwrap()
        .into_inner();

    let mut got = Vec::new();
    let collect = async {
        for _ in 0..2 {
            match tokio::time::timeout(Duration::from_secs(5), stream.message()).await {
                Ok(Ok(Some(t))) => got.push(t.payload),
                _ => break,
            }
        }
    };
    collect.await;
    assert_eq!(got, vec![vec![0u8], vec![1u8]]);
}

#[tokio::test]
async fn grpc_per_queue_isolation() {
    let (_addr, mut client, _handle) = start().await;

    client
        .add_task(AddTaskRequest {
            tenant: "acme".into(),
            task_queue: "a".into(),
            task_type: String::new(),
            payload: b"from-a".to_vec(),
            visibility_timeout_ms: 0,
        })
        .await
        .unwrap();
    client
        .add_task(AddTaskRequest {
            tenant: "acme".into(),
            task_queue: "b".into(),
            task_type: String::new(),
            payload: b"from-b".to_vec(),
            visibility_timeout_ms: 0,
        })
        .await
        .unwrap();

    let ta = client
        .poll_task(PollTaskRequest {
            tenant: "acme".into(),
            task_queue: "a".into(),
            worker_id: String::new(),
            task_type: String::new(),
            long_poll_timeout_ms: 1000,
        })
        .await
        .unwrap()
        .into_inner()
        .task
        .unwrap();
    assert_eq!(ta.payload, b"from-a");
}
