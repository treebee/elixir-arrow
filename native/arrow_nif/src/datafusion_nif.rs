use crate::RecordBatchResource;
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use rustler::ResourceArc;
use std::sync::{Arc, Mutex};
use tokio::runtime::Runtime;

pub struct ExecutionContextResource(Mutex<ExecutionContext>);

async fn fetch_results(df: Arc<dyn DataFrame>) -> Vec<RecordBatch> {
    let r = df.collect().await;
    //let results: Vec<RecordBatch> = df.collect().await;
    r.unwrap()
}

#[rustler::nif]
fn query_parquet(
    path: String,
    table: String,
    query: String,
) -> Vec<ResourceArc<RecordBatchResource>> {
    let mut ctx = ExecutionContext::new();
    ctx.register_parquet(table.as_str(), path.as_str()).unwrap();
    let df = ctx.sql(query.as_str()).unwrap();
    let mut rt = Runtime::new().unwrap();
    let results: Vec<RecordBatch> = { rt.block_on(async { fetch_results(df).await }) };
    let mut batch_resources = Vec::new();
    for batch in results {
        batch_resources.push(ResourceArc::new(RecordBatchResource(batch)));
    }
    batch_resources
}

#[rustler::nif]
fn create_datafusion_execution_context() -> ResourceArc<ExecutionContextResource> {
    let ctx = ExecutionContext::new();
    ResourceArc::new(ExecutionContextResource(Mutex::new(ctx)))
}

#[rustler::nif]
fn datafusion_execution_context_register_parquet(
    ctx: ResourceArc<ExecutionContextResource>,
    table: String,
    path: String,
) {
    let mut context = ctx.0.lock().unwrap();
    context
        .register_parquet(table.as_str(), path.as_str())
        .unwrap();
}

#[rustler::nif]
fn datafusion_execution_context_register_csv(
    ctx: ResourceArc<ExecutionContextResource>,
    table: String,
    path: String,
) {
    let mut context = ctx.0.lock().unwrap();
    context
        .register_csv(table.as_str(), path.as_str(), CsvReadOptions::new())
        .unwrap();
}

#[rustler::nif]
fn datafusion_execute_sql(
    ctx: ResourceArc<ExecutionContextResource>,
    query: String,
) -> Vec<ResourceArc<RecordBatchResource>> {
    let mut context = ctx.0.lock().unwrap();
    let df = context.sql(query.as_str()).unwrap();
    let mut rt = Runtime::new().unwrap();
    let results: Vec<RecordBatch> = { rt.block_on(async { fetch_results(df).await }) };
    let mut batch_resources = Vec::new();
    for batch in results {
        batch_resources.push(ResourceArc::new(RecordBatchResource(batch)));
    }
    batch_resources
}
