use crate::schema::XSchema;
use arrow::array::Float32Array;
use arrow::array::Int32Array;
use arrow::array::UInt32Array;
use arrow::array::{ArrayRef, Float64Array, Int64Array};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use rustler::ResourceArc;
use rustler::Term;
use std::sync::Arc;

pub struct RecordBatchResource(pub RecordBatch);

#[rustler::nif]
fn print_record_batch(table: ResourceArc<RecordBatchResource>) {
    let t = &table.0;
    println!("{:?}", t);
}

#[rustler::nif]
fn make_record_batch<'a>(
    schema: XSchema,
    columns: Vec<Term<'a>>,
) -> ResourceArc<RecordBatchResource> {
    let arrow_schema = schema.to_arrow();
    let mut cols: Vec<ArrayRef> = Vec::new();
    for (idx, field) in arrow_schema.fields().iter().enumerate() {
        match field.data_type() {
            DataType::Int32 => {
                cols.push(Arc::new(Int32Array::from(
                    columns[idx].decode::<Vec<Option<i32>>>().unwrap(),
                )));
            }
            DataType::Int64 => {
                cols.push(Arc::new(Int64Array::from(
                    columns[idx].decode::<Vec<Option<i64>>>().unwrap(),
                )));
            }
            DataType::UInt32 => {
                cols.push(Arc::new(UInt32Array::from(
                    columns[idx].decode::<Vec<Option<u32>>>().unwrap(),
                )));
            }
            DataType::Float32 => {
                cols.push(Arc::new(Float32Array::from(
                    columns[idx].decode::<Vec<Option<f32>>>().unwrap(),
                )));
            }
            DataType::Float64 => {
                cols.push(Arc::new(Float64Array::from(
                    columns[idx].decode::<Vec<Option<f64>>>().unwrap(),
                )));
            }
            _ => println!("no match"),
        }
    }
    ResourceArc::new(RecordBatchResource(
        RecordBatch::try_new(Arc::new(arrow_schema), cols).unwrap(),
    ))
}

#[rustler::nif]
fn get_schema(table: ResourceArc<RecordBatchResource>) -> XSchema {
    XSchema::from_arrow(&table.0.schema())
}
