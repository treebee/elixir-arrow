use crate::schema::XSchema;
use arrow::array::{ArrayRef, Float64Array, Int64Array};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use rustler::ResourceArc;
use std::sync::Arc;

pub struct TableResource(pub RecordBatch);

#[rustler::nif]
fn get_table(schema: XSchema) -> ResourceArc<TableResource> {
    let s = schema.to_arrow();
    let mut columns: Vec<ArrayRef> = Vec::new();
    for field in s.fields() {
        match field.data_type() {
            &DataType::Int64 => columns.push(Arc::new(Int64Array::from(vec![1, 2]))),
            &DataType::Float64 => columns.push(Arc::new(Float64Array::from(vec![1.2, 42.0]))),
            _ => println!("no match"),
        }
    }
    ResourceArc::new(TableResource(
        RecordBatch::try_new(Arc::new(s), columns).unwrap(),
    ))
}

#[rustler::nif]
fn print_table(table: ResourceArc<TableResource>) {
    let t = &table.0;
    let s = t.schema();
    println!("{:?}", s);
    println!("{:?}", t);
}
