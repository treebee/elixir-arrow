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

pub struct TableResource(pub RecordBatch);

#[rustler::nif]
fn get_table(schema: XSchema) -> ResourceArc<TableResource> {
    let s = schema.to_arrow();
    let mut columns: Vec<ArrayRef> = Vec::new();
    for field in s.fields() {
        match field.data_type() {
            &DataType::Int64 => {
                columns.push(Arc::new(Int64Array::from(vec![Some(1), Some(2), None])))
            }
            &DataType::Float64 => columns.push(Arc::new(Float64Array::from(vec![
                Some(1.2),
                None,
                Some(42.0),
            ]))),
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
    println!("{:?}", t);
}

#[rustler::nif]
fn make_table<'a>(schema: XSchema, columns: Vec<Term<'a>>) -> ResourceArc<TableResource> {
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
    ResourceArc::new(TableResource(
        RecordBatch::try_new(Arc::new(arrow_schema), cols).unwrap(),
    ))
}

#[rustler::nif]
fn get_schema(table: ResourceArc<TableResource>) -> XSchema {
    XSchema::from_arrow(&table.0.schema())
}
