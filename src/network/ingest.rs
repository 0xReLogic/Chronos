use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{mpsc::UnboundedReceiver, Mutex};
use tokio::time::{sleep, Duration};

use crate::executor::Executor;
use crate::parser::{Ast, Statement, Value};

#[derive(Debug, Clone, serde::Deserialize)]
pub struct IngestRequest {
    pub device_id: String,
    pub ts: i64,
    pub seq: u64,
    pub metrics: HashMap<String, f64>,
}

pub async fn run_ingest_worker(
    mut rx: UnboundedReceiver<IngestRequest>,
    executor: Arc<Mutex<Executor>>,
) {
    while let Some(msg) = rx.recv().await {
        for (metric, value) in msg.metrics.iter() {
            let mut attempts: u32 = 0;
            let max_attempts: u32 = 5;

            loop {
                attempts += 1;

                let ast = build_insert_ast(&msg, metric, *value);

                let result = {
                    let mut exec = executor.lock().await;
                    exec.execute(ast).await
                };

                match result {
                    Ok(_) => {
                        break;
                    }
                    Err(e) => {
                        log::error!(
                            "Ingest worker: failed to insert reading device_id={} ts={} seq={} metric={} on attempt {}: {}",
                            msg.device_id,
                            msg.ts,
                            msg.seq,
                            metric,
                            attempts,
                            e
                        );
                        if attempts >= max_attempts {
                            log::error!(
                                "Ingest worker: giving up on reading device_id={} ts={} seq={} metric={} after {} attempts",
                                msg.device_id,
                                msg.ts,
                                msg.seq,
                                metric,
                                attempts
                            );
                            break;
                        }
                        sleep(Duration::from_millis(200)).await;
                    }
                }
            }
        }
    }
}

fn build_insert_ast(msg: &IngestRequest, metric: &str, value: f64) -> Ast {
    let table_name = "readings".to_string();
    let reading_id = format!("{}:{}:{}:{}", msg.device_id, msg.ts, msg.seq, metric);

    let columns = Some(vec![
        "reading_id".to_string(),
        "device_id".to_string(),
        "ts".to_string(),
        "seq".to_string(),
        "metric".to_string(),
        "value".to_string(),
    ]);

    let values = vec![
        Value::String(reading_id),
        Value::String(msg.device_id.clone()),
        Value::Integer(msg.ts),
        Value::Integer(msg.seq as i64),
        Value::String(metric.to_string()),
        Value::Float(value),
    ];

    Ast::Statement(Statement::Insert {
        table_name,
        columns,
        values,
    })
}
