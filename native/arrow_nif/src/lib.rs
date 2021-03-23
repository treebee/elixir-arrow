use rustler::{Env, Term};

mod array;
mod datafusion;
mod datatype;
mod field;
mod parquet;
mod record_batch;
mod schema;

use crate::array::{array_data_type, array_slice, len, make_array, sum, to_list, XArrayRef};
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
        array_slice,
        make_array,
        sum,
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
