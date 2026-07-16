//! `tonic` implementation of the `MatchingService` gRPC facade.

use std::time::Duration;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::dispatch::{AckResult, TaskQueueManager};
use crate::error::WorkflowError;
use crate::proto::synapse::workflow::v1::matching_service_server::MatchingService;
use crate::proto::synapse::workflow::v1::{
    AddTaskRequest, AddTaskResponse, PollTaskRequest, PollTaskResponse, RespondRequest,
    RespondResponse, StreamTasksRequest, Task,
};

/// Default long-poll ceiling used by the streaming pull between deliveries.
const STREAM_POLL_TIMEOUT: Duration = Duration::from_secs(5);

/// The matching-service gRPC front end. Holds an `Arc<TaskQueueManager>` that
/// does the real work against the `Queue` primitive.
#[derive(Clone)]
pub struct MatchingServiceImpl {
    manager: std::sync::Arc<TaskQueueManager>,
}

impl MatchingServiceImpl {
    pub fn new(manager: std::sync::Arc<TaskQueueManager>) -> Self {
        Self { manager }
    }

    pub fn manager(&self) -> &std::sync::Arc<TaskQueueManager> {
        &self.manager
    }
}

fn to_status(e: WorkflowError) -> Status {
    match e {
        WorkflowError::Engine(ee) => match ee.kind() {
            synapse_core::ErrorKind::NotFound => Status::not_found(ee.to_string()),
            synapse_core::ErrorKind::InvalidArgument => Status::invalid_argument(ee.to_string()),
            synapse_core::ErrorKind::AlreadyExists => Status::already_exists(ee.to_string()),
            synapse_core::ErrorKind::TenantQuotaExceeded => {
                Status::resource_exhausted(ee.to_string())
            }
            synapse_core::ErrorKind::Closed => Status::unavailable(ee.to_string()),
            _ => Status::internal(ee.to_string()),
        },
        WorkflowError::InvalidArgument(m) => Status::invalid_argument(m),
        WorkflowError::UnknownToken => Status::not_found("unknown task token"),
        WorkflowError::Internal(m) => Status::internal(m),
    }
}

fn opt_type(s: &str) -> Option<&str> {
    if s.is_empty() {
        None
    } else {
        Some(s)
    }
}

fn timeout_ms(ms: i64) -> Duration {
    if ms <= 0 {
        Duration::ZERO
    } else {
        Duration::from_millis(ms as u64)
    }
}

#[tonic::async_trait]
impl MatchingService for MatchingServiceImpl {
    async fn add_task(
        &self,
        request: Request<AddTaskRequest>,
    ) -> Result<Response<AddTaskResponse>, Status> {
        let req = request.into_inner();
        let visibility = if req.visibility_timeout_ms > 0 {
            Some(Duration::from_millis(req.visibility_timeout_ms as u64))
        } else {
            None
        };
        let seq = self
            .manager
            .add_task(
                &req.tenant,
                &req.task_queue,
                opt_type(&req.task_type),
                &req.payload,
                visibility,
            )
            .map_err(to_status)?;
        Ok(Response::new(AddTaskResponse { seq }))
    }

    async fn poll_task(
        &self,
        request: Request<PollTaskRequest>,
    ) -> Result<Response<PollTaskResponse>, Status> {
        let req = request.into_inner();
        let task = self
            .manager
            .poll(
                &req.tenant,
                &req.task_queue,
                opt_type(&req.task_type),
                timeout_ms(req.long_poll_timeout_ms),
            )
            .await
            .map_err(to_status)?;
        Ok(Response::new(PollTaskResponse { task }))
    }

    type StreamTasksStream = ReceiverStream<Result<Task, Status>>;

    async fn stream_tasks(
        &self,
        request: Request<StreamTasksRequest>,
    ) -> Result<Response<Self::StreamTasksStream>, Status> {
        let req = request.into_inner();
        let tenant = req.tenant;
        let task_queue = req.task_queue;
        let task_type = opt_type(&req.task_type).map(|s| s.to_string());
        let visibility = if req.visibility_timeout_ms > 0 {
            Some(Duration::from_millis(req.visibility_timeout_ms as u64))
        } else {
            None
        };

        let mgr = self.manager.clone();
        if let Some(v) = visibility {
            let tt: Option<&str> = task_type.as_deref();
            mgr.set_visibility(&tenant, &task_queue, tt, v)
                .map_err(to_status)?;
        }

        let (tx, rx) = mpsc::channel(16);
        tokio::spawn(async move {
            let tt: Option<&str> = task_type.as_deref();
            loop {
                match mgr
                    .poll(&tenant, &task_queue, tt, STREAM_POLL_TIMEOUT)
                    .await
                {
                    Ok(Some(task)) => {
                        if tx.send(Ok(task)).await.is_err() {
                            return; // client disconnected
                        }
                    }
                    Ok(None) => {
                        // Long-poll timed out with no task; keep the stream alive
                        // and wait for the next one.
                        continue;
                    }
                    Err(e) => {
                        let _ = tx.send(Err(to_status(e))).await;
                        return;
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn respond_task_completed(
        &self,
        request: Request<RespondRequest>,
    ) -> Result<Response<RespondResponse>, Status> {
        let req = request.into_inner();
        let res = self
            .manager
            .respond(&req.task_token, false)
            .map_err(to_status)?;
        Ok(Response::new(map_ack(res)))
    }

    async fn respond_task_failed(
        &self,
        request: Request<RespondRequest>,
    ) -> Result<Response<RespondResponse>, Status> {
        let req = request.into_inner();
        let res = self
            .manager
            .respond(&req.task_token, true)
            .map_err(to_status)?;
        Ok(Response::new(map_ack(res)))
    }
}

fn map_ack(res: AckResult) -> RespondResponse {
    match res {
        AckResult::Accepted => RespondResponse {
            accepted: true,
            duplicate: false,
        },
        AckResult::Duplicate => RespondResponse {
            accepted: true,
            duplicate: true,
        },
        AckResult::Unknown => RespondResponse {
            accepted: false,
            duplicate: false,
        },
    }
}
