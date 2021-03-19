use crate::array::StringArrayResource;
use rustler::{Env, Term};

mod array;
mod datatype;
mod field;
mod parquet_ex;
mod schema;
mod table;

use crate::array::{
    len, make_array, sum, to_list, ArrayResource, Float32ArrayResource, Float64ArrayResource,
    Int16ArrayResource, Int32ArrayResource, Int64ArrayResource, Int8ArrayResource,
    UInt16ArrayResource, UInt32ArrayResource, UInt64ArrayResource, UInt8ArrayResource,
};
use crate::parquet_ex::{
    next_batch, parquet_reader, parquet_reader_arrow_schema, parquet_schema, record_reader,
    write_record_batches, ParquetReaderResource, ParquetRecordBatchReaderResource,
    RecordBatchesResource,
};
use crate::table::{get_schema, make_record_batch, print_record_batch, RecordBatchResource};

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
    rustler::resource!(UInt8ArrayResource, env);
    rustler::resource!(UInt16ArrayResource, env);
    rustler::resource!(UInt32ArrayResource, env);
    rustler::resource!(UInt64ArrayResource, env);
    rustler::resource!(Int8ArrayResource, env);
    rustler::resource!(Int16ArrayResource, env);
    rustler::resource!(Int32ArrayResource, env);
    rustler::resource!(Int64ArrayResource, env);
    rustler::resource!(Float64ArrayResource, env);
    rustler::resource!(Float32ArrayResource, env);
    rustler::resource!(StringArrayResource, env);
    rustler::resource!(ArrayResource, env);
    rustler::resource!(RecordBatchResource, env);
    rustler::resource!(ParquetReaderResource, env);
    rustler::resource!(RecordBatchesResource, env);
    rustler::resource!(ParquetRecordBatchReaderResource, env);
    on_load(env);
    true
}

rustler::init!(
    "Elixir.Arrow",
    [
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
    ],
    load = load
);
