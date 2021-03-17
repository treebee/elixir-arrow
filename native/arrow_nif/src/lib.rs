use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use rustler::{Env, Term};
use std::sync::Arc;

mod array;
mod datatype;
mod field;
mod schema;
mod table;

use crate::array::{
    len, make_array, sum, to_list, ArrayResource, Float32ArrayResource, Float64ArrayResource,
    Int32ArrayResource, Int64ArrayResource, UInt32ArrayResource,
};
use crate::field::XField;
use crate::schema::{MetaData, XSchema};
use crate::table::{get_table, print_table, TableResource};

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
    }
}

#[rustler::nif]
fn get_schema() -> XSchema {
    let f = Field::new("my_field", DataType::Int64, false);
    let xf = XField::from_arrow(f);
    XSchema {
        fields: vec![xf],
        metadata: MetaData::new(),
    }
}

#[rustler::nif]
fn echo_schema(schema: XSchema) -> XSchema {
    let s = schema.to_arrow();
    let rb = RecordBatch::try_new(
        Arc::new(s.clone()),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]))],
    );
    println!("\nschema\n{:?}", s);
    println!("\nbatch\n{:?}", rb);
    schema
}

#[rustler::nif]
fn get_field() -> XField {
    let f = Field::new("my_field", DataType::Int64, false);
    XField::from_arrow(f)
}

#[rustler::nif]
fn echo_field(field: XField) -> XField {
    let arrow_field = field.to_arrow();
    println!("field: {:?}", arrow_field);
    field
}

pub fn on_load(_env: Env) -> bool {
    true
}

fn load(env: Env, _: Term) -> bool {
    rustler::resource!(UInt32ArrayResource, env);
    rustler::resource!(Int32ArrayResource, env);
    rustler::resource!(Int64ArrayResource, env);
    rustler::resource!(Float64ArrayResource, env);
    rustler::resource!(Float32ArrayResource, env);
    rustler::resource!(ArrayResource, env);
    rustler::resource!(TableResource, env);
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
        get_field,
        echo_field,
        get_schema,
        echo_schema,
        get_table,
        print_table,
    ],
    load = load
);
