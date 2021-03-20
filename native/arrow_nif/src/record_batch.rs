use crate::array::ArrayValues;
use crate::schema::XSchema;
use arrow::array::Float32Array;
use arrow::array::Int16Array;
use arrow::array::Int32Array;
use arrow::array::Int8Array;
use arrow::array::StringArray;
use arrow::array::UInt16Array;
use arrow::array::UInt32Array;
use arrow::array::UInt64Array;
use arrow::array::UInt8Array;
use arrow::array::{ArrayRef, Float64Array, Int64Array};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use rustler::Encoder;
use rustler::Env;
use rustler::ResourceArc;
use rustler::Term;
use std::collections::HashMap;
use std::sync::Arc;

pub struct RecordBatchResource(pub RecordBatch);
pub struct RecordBatchStruct(HashMap<String, ArrayValues>);

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
            DataType::Utf8 => {
                let data = columns[idx].decode::<Vec<Option<String>>>().unwrap();
                let values: Vec<&str> = data
                    .iter()
                    .map(|s| match s {
                        Some(t) => t.as_str(),
                        None => "",
                    })
                    .collect();
                cols.push(Arc::new(StringArray::from(values)));
            }

            _ => println!("no match"),
        }
    }
    ResourceArc::new(RecordBatchResource(
        RecordBatch::try_new(Arc::new(arrow_schema), cols).unwrap(),
    ))
}

#[rustler::nif]
fn get_schema(record_batch: ResourceArc<RecordBatchResource>) -> XSchema {
    XSchema::from_arrow(&record_batch.0.schema())
}

#[rustler::nif]
fn record_batch_to_map(record_batch: ResourceArc<RecordBatchResource>) -> RecordBatchStruct {
    let batch = &record_batch.0;
    let mut ret = HashMap::new();
    for field in batch.schema().fields() {
        let column = batch
            .column(batch.schema().index_of(field.name()).unwrap())
            .as_any();
        let array_values = match field.data_type() {
            DataType::Int8 => ArrayValues::Int8(
                column
                    .downcast_ref::<Int8Array>()
                    .expect("Failed to downcast")
                    .into_iter()
                    .collect(),
            ),
            DataType::Int16 => ArrayValues::Int16(
                column
                    .downcast_ref::<Int16Array>()
                    .expect("Failed to downcast")
                    .into_iter()
                    .collect(),
            ),
            DataType::Int32 => ArrayValues::Int32(
                column
                    .downcast_ref::<Int32Array>()
                    .expect("Failed to downcast")
                    .into_iter()
                    .collect(),
            ),
            DataType::Int64 => ArrayValues::Int64(
                column
                    .downcast_ref::<Int64Array>()
                    .expect("Failed to downcast")
                    .into_iter()
                    .collect(),
            ),
            DataType::UInt8 => ArrayValues::UInt8(
                column
                    .downcast_ref::<UInt8Array>()
                    .expect("Failed to downcast")
                    .into_iter()
                    .collect(),
            ),
            DataType::UInt16 => ArrayValues::UInt16(
                column
                    .downcast_ref::<UInt16Array>()
                    .expect("Failed to downcast")
                    .into_iter()
                    .collect(),
            ),
            DataType::UInt32 => ArrayValues::UInt32(
                column
                    .downcast_ref::<UInt32Array>()
                    .expect("Failed to downcast")
                    .into_iter()
                    .collect(),
            ),
            DataType::UInt64 => ArrayValues::UInt64(
                column
                    .downcast_ref::<UInt64Array>()
                    .expect("Failed to downcast")
                    .into_iter()
                    .collect(),
            ),
            DataType::Float32 => ArrayValues::Float32(
                column
                    .downcast_ref::<Float32Array>()
                    .expect("Failed to downcast")
                    .into_iter()
                    .collect(),
            ),
            DataType::Float64 => ArrayValues::Float64(
                column
                    .downcast_ref::<Float64Array>()
                    .expect("Failed to downcast")
                    .into_iter()
                    .collect(),
            ),
            DataType::Utf8 => {
                let mut values: Vec<Option<String>> = Vec::new();
                for value in column
                    .downcast_ref::<StringArray>()
                    .expect("Failed to downcast")
                    .into_iter()
                {
                    match value {
                        Some(t) => values.push(Some(String::from(t))),
                        None => values.push(Some(String::from(""))),
                    }
                }
                ArrayValues::Utf8(values)
            }
            // TODO error handling
            _ => ArrayValues::Int64(vec![]),
        };
        ret.insert(field.name().clone(), array_values);
    }
    RecordBatchStruct(ret)
}

impl Encoder for RecordBatchStruct {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        let mut k = Vec::new();
        let mut v = Vec::new();
        for (key, value) in &self.0 {
            k.push(key.encode(env));
            v.push(value.encode(env));
        }
        Term::map_from_arrays(env, &k, &v).unwrap()
    }
}
