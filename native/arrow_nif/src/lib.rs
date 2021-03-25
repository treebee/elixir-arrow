use rustler::{Env, Term};

mod array;
mod compute;
mod datafusion;
mod datatype;
mod field;
mod parquet;
mod record_batch;
mod schema;

use crate::array::{
    array_data_type, array_is_empty, array_is_null, array_is_valid, array_null_count, array_offset,
    array_slice, len, make_array, to_list, XArrayRef,
};
use crate::compute::aggregate::array_sum;
use crate::compute::arithmetic::{
    array_compute_add, array_compute_divide, array_compute_multiply, array_compute_subtract,
};
use crate::compute::comparison::{
    array_compute_eq, array_compute_eq_scalar, array_compute_eq_utf8, array_compute_eq_utf8_scalar,
    array_compute_gt, array_compute_gt_eq, array_compute_gt_eq_scalar, array_compute_gt_eq_utf8,
    array_compute_gt_eq_utf8_scalar, array_compute_gt_scalar, array_compute_gt_utf8,
    array_compute_gt_utf8_scalar, array_compute_like_utf8, array_compute_like_utf8_scalar,
    array_compute_lt, array_compute_lt_eq, array_compute_lt_eq_scalar, array_compute_lt_eq_utf8,
    array_compute_lt_eq_utf8_scalar, array_compute_lt_scalar, array_compute_lt_utf8,
    array_compute_lt_utf8_scalar, array_compute_neq, array_compute_neq_scalar,
    array_compute_neq_utf8, array_compute_neq_utf8_scalar, array_compute_nlike_utf8,
    array_compute_nlike_utf8_scalar,
};
use crate::datafusion::{
    create_datafusion_execution_context, datafusion_execute_sql,
    datafusion_execution_context_register_csv, datafusion_execution_context_register_parquet,
    query_parquet, ExecutionContextResource,
};
use crate::parquet::{
    next_batch, parquet_reader, parquet_reader_arrow_schema, parquet_schema, record_reader,
    write_record_batches, ParquetReaderResource, ParquetRecordBatchReaderResource,
    RecordBatchesResource,
};
use crate::record_batch::{
    get_schema, make_record_batch, print_record_batch, record_batch_to_map, RecordBatchResource,
};

mod atoms {
    rustler::atoms! {
        // standard atoms
        ok,
        error,

        // error atoms
        unsupported_type,

        // type atoms
        s,
        f,
        u,
        utf8,
    }
}

pub fn on_load(_env: Env) -> bool {
    true
}

fn load(env: Env, _: Term) -> bool {
    rustler::resource!(RecordBatchResource, env);
    rustler::resource!(ParquetReaderResource, env);
    rustler::resource!(RecordBatchesResource, env);
    rustler::resource!(ParquetRecordBatchReaderResource, env);
    rustler::resource!(ExecutionContextResource, env);
    rustler::resource!(XArrayRef, env);
    on_load(env);
    true
}

rustler::init!(
    "Elixir.Arrow",
    [
        array_data_type,
        array_is_empty,
        array_is_null,
        array_is_valid,
        array_null_count,
        array_offset,
        array_slice,
        array_compute_add,
        array_compute_divide,
        array_compute_multiply,
        array_compute_subtract,
        array_compute_eq,
        array_compute_neq,
        array_compute_gt,
        array_compute_gt_eq,
        array_compute_lt,
        array_compute_lt_eq,
        array_compute_eq_utf8,
        array_compute_neq_utf8,
        array_compute_gt_utf8,
        array_compute_like_utf8,
        array_compute_nlike_utf8,
        array_compute_gt_eq_utf8,
        array_compute_lt_utf8,
        array_compute_lt_eq_utf8,
        array_compute_eq_scalar,
        array_compute_neq_scalar,
        array_compute_gt_scalar,
        array_compute_gt_eq_scalar,
        array_compute_lt_scalar,
        array_compute_lt_eq_scalar,
        array_compute_eq_utf8_scalar,
        array_compute_neq_utf8_scalar,
        array_compute_gt_utf8_scalar,
        array_compute_gt_eq_utf8_scalar,
        array_compute_lt_utf8_scalar,
        array_compute_lt_eq_utf8_scalar,
        array_compute_like_utf8_scalar,
        array_compute_nlike_utf8_scalar,
        make_array,
        array_sum,
        len,
        to_list,
        get_schema,
        print_record_batch,
        make_record_batch,
        parquet_reader,
        parquet_reader_arrow_schema,
        parquet_schema,
        record_reader,
        next_batch,
        write_record_batches,
        record_batch_to_map,
        query_parquet,
        create_datafusion_execution_context,
        datafusion_execute_sql,
        datafusion_execution_context_register_parquet,
        datafusion_execution_context_register_csv
    ],
    load = load
);
