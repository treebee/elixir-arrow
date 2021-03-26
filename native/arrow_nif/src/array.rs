use crate::datatype::XDataType;
use arrow::array::{
    ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, StringArray, TimestampMicrosecondArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow::datatypes::{DataType, TimeUnit};
use rustler::Env;
use rustler::Term;
use rustler::{Encoder, NifStruct, ResourceArc};
use std::sync::Arc;

pub struct XArrayRef(pub ArrayRef);

#[derive(NifStruct)]
#[module = "Arrow.Array"]
pub struct ArrayResource {
    pub reference: ResourceArc<XArrayRef>,
}

pub enum PrimitiveValue {
    Boolean(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    Utf8(Option<String>),
}

pub enum ArrayValues {
    Boolean(Vec<Option<bool>>),
    Int8(Vec<Option<i8>>),
    Int16(Vec<Option<i16>>),
    Int32(Vec<Option<i32>>),
    Int64(Vec<Option<i64>>),
    UInt8(Vec<Option<u8>>),
    UInt16(Vec<Option<u16>>),
    UInt32(Vec<Option<u32>>),
    UInt64(Vec<Option<u64>>),
    Float32(Vec<Option<f32>>),
    Float64(Vec<Option<f64>>),
    Utf8(Vec<Option<String>>),
    Timestamp(Vec<Option<i64>>),
}

impl Encoder for PrimitiveValue {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        match self {
            PrimitiveValue::Int8(v) => v.encode(env),
            PrimitiveValue::Int16(v) => v.encode(env),
            PrimitiveValue::Int32(v) => v.encode(env),
            PrimitiveValue::Int64(v) => v.encode(env),
            PrimitiveValue::UInt8(v) => v.encode(env),
            PrimitiveValue::UInt16(v) => v.encode(env),
            PrimitiveValue::UInt32(v) => v.encode(env),
            PrimitiveValue::UInt64(v) => v.encode(env),
            PrimitiveValue::Float32(v) => v.encode(env),
            PrimitiveValue::Float64(v) => v.encode(env),
            PrimitiveValue::Utf8(v) => v.encode(env),
            PrimitiveValue::Boolean(v) => v.encode(env),
        }
    }
}

impl Encoder for ArrayValues {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        match self {
            ArrayValues::Boolean(v) => v.encode(env),
            ArrayValues::Int8(v) => v.encode(env),
            ArrayValues::Int16(v) => v.encode(env),
            ArrayValues::Int32(v) => v.encode(env),
            ArrayValues::Int64(v) => v.encode(env),
            ArrayValues::UInt8(v) => v.encode(env),
            ArrayValues::UInt16(v) => v.encode(env),
            ArrayValues::UInt32(v) => v.encode(env),
            ArrayValues::UInt64(v) => v.encode(env),
            ArrayValues::Float32(v) => v.encode(env),
            ArrayValues::Float64(v) => v.encode(env),
            ArrayValues::Utf8(v) => v.encode(env),
            ArrayValues::Timestamp(v) => v.encode(env),
        }
    }
}

#[rustler::nif]
fn make_array(a: Term, b: XDataType) -> ArrayResource {
    match &b.0 {
        DataType::Boolean => {
            let values: Vec<Option<bool>> = a.decode().unwrap();
            ArrayResource {
                reference: ResourceArc::new(XArrayRef(
                    Arc::new(BooleanArray::from(values)) as ArrayRef
                )),
            }
        }
        DataType::Int8 => {
            let values: Vec<Option<i8>> = a.decode().unwrap();
            ArrayResource {
                reference: ResourceArc::new(XArrayRef(
                    Arc::new(Int8Array::from(values)) as ArrayRef
                )),
            }
        }
        DataType::Int16 => {
            let values: Vec<Option<i16>> = a.decode().unwrap();
            ArrayResource {
                reference: ResourceArc::new(XArrayRef(
                    Arc::new(Int16Array::from(values)) as ArrayRef
                )),
            }
        }
        DataType::Int32 => {
            let values: Vec<Option<i32>> = a.decode().unwrap();
            ArrayResource {
                reference: ResourceArc::new(XArrayRef(
                    Arc::new(Int32Array::from(values)) as ArrayRef
                )),
            }
        }
        DataType::Int64 => {
            let values: Vec<Option<i64>> = a.decode().unwrap();
            ArrayResource {
                reference: ResourceArc::new(XArrayRef(
                    Arc::new(Int64Array::from(values)) as ArrayRef
                )),
            }
        }
        DataType::UInt8 => {
            let values: Vec<Option<u8>> = a.decode().unwrap();
            ArrayResource {
                reference: ResourceArc::new(XArrayRef(
                    Arc::new(UInt8Array::from(values)) as ArrayRef
                )),
            }
        }
        DataType::UInt16 => {
            let values: Vec<Option<u16>> = a.decode().unwrap();
            ArrayResource {
                reference: ResourceArc::new(XArrayRef(
                    Arc::new(UInt16Array::from(values)) as ArrayRef
                )),
            }
        }
        DataType::UInt32 => {
            let values: Vec<Option<u32>> = a.decode().unwrap();
            ArrayResource {
                reference: ResourceArc::new(XArrayRef(
                    Arc::new(UInt32Array::from(values)) as ArrayRef
                )),
            }
        }
        DataType::UInt64 => {
            let values: Vec<Option<u64>> = a.decode().unwrap();
            ArrayResource {
                reference: ResourceArc::new(XArrayRef(
                    Arc::new(UInt64Array::from(values)) as ArrayRef
                )),
            }
        }
        DataType::Float32 => {
            let values: Vec<Option<f32>> = a.decode().unwrap();
            ArrayResource {
                reference: ResourceArc::new(XArrayRef(
                    Arc::new(Float32Array::from(values)) as ArrayRef
                )),
            }
        }
        DataType::Float64 => {
            let values: Vec<Option<f64>> = a.decode().unwrap();
            ArrayResource {
                reference: ResourceArc::new(XArrayRef(
                    Arc::new(Float64Array::from(values)) as ArrayRef
                )),
            }
        }
        DataType::Utf8 => {
            let values: Vec<Option<String>> = a.decode().unwrap();
            let values: Vec<Option<&str>> = values
                .iter()
                .map(|s| match s {
                    Some(v) => Some(v.as_str()),
                    None => None,
                })
                .collect();
            ArrayResource {
                reference: ResourceArc::new(XArrayRef(
                    Arc::new(StringArray::from(values)) as ArrayRef
                )),
            }
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            let values: Vec<Option<i64>> = a.decode().unwrap();
            ArrayResource {
                reference: ResourceArc::new(XArrayRef(Arc::new(
                    TimestampMicrosecondArray::from_opt_vec(values, None),
                ) as ArrayRef)),
            }
        }
        dtype => panic!("arrays with datatype {} not supported", dtype),
    }
}

#[rustler::nif]
fn to_list(arr: ArrayResource) -> ArrayValues {
    match &arr.reference.0.data_type() {
        DataType::Boolean => ArrayValues::Boolean(
            arr.reference
                .0
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .into_iter()
                .collect(),
        ),
        DataType::Int8 => ArrayValues::Int8(
            arr.reference
                .0
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .into_iter()
                .collect(),
        ),
        DataType::Int16 => ArrayValues::Int16(
            arr.reference
                .0
                .as_any()
                .downcast_ref::<Int16Array>()
                .unwrap()
                .into_iter()
                .collect(),
        ),
        DataType::Int32 => ArrayValues::Int32(
            arr.reference
                .0
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .into_iter()
                .collect(),
        ),
        DataType::Int64 => ArrayValues::Int64(
            arr.reference
                .0
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .into_iter()
                .collect(),
        ),
        DataType::UInt8 => ArrayValues::UInt8(
            arr.reference
                .0
                .as_any()
                .downcast_ref::<UInt8Array>()
                .unwrap()
                .into_iter()
                .collect(),
        ),
        DataType::UInt16 => ArrayValues::UInt16(
            arr.reference
                .0
                .as_any()
                .downcast_ref::<UInt16Array>()
                .unwrap()
                .into_iter()
                .collect(),
        ),
        DataType::UInt32 => ArrayValues::UInt32(
            arr.reference
                .0
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .into_iter()
                .collect(),
        ),
        DataType::UInt64 => ArrayValues::UInt64(
            arr.reference
                .0
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .into_iter()
                .collect(),
        ),
        DataType::Float32 => ArrayValues::Float32(
            arr.reference
                .0
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .into_iter()
                .collect(),
        ),
        DataType::Float64 => ArrayValues::Float64(
            arr.reference
                .0
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .into_iter()
                .collect(),
        ),
        DataType::Utf8 => ArrayValues::Utf8(
            arr.reference
                .0
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .map(|s| match s {
                    Some(t) => Some(String::from(t)),
                    None => None,
                })
                .collect(),
        ),
        DataType::Timestamp(_, _) => ArrayValues::Timestamp(
            arr.reference
                .0
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .into_iter()
                .collect(),
        ),
        dtype => panic!("datatype {} is not supported", dtype),
    }
}

#[rustler::nif]
fn len(arr: ArrayResource) -> usize {
    arr.reference.0.len()
}

#[rustler::nif]
fn array_data_type(arr: ArrayResource) -> XDataType {
    XDataType::from_arrow(&arr.reference.0.data_type())
}

#[rustler::nif]
fn array_slice(arr: ArrayResource, offset: usize, length: usize) -> ArrayResource {
    let slice = arr.reference.0.slice(offset, length);
    ArrayResource {
        reference: ResourceArc::new(XArrayRef(slice)),
    }
}

#[rustler::nif]
fn array_is_empty(arr: ArrayResource) -> bool {
    arr.reference.0.is_empty()
}

#[rustler::nif]
fn array_offset(arr: ArrayResource) -> usize {
    arr.reference.0.offset()
}

#[rustler::nif]
fn array_is_valid(arr: ArrayResource, index: usize) -> bool {
    arr.reference.0.is_valid(index)
}

#[rustler::nif]
fn array_is_null(arr: ArrayResource, index: usize) -> bool {
    arr.reference.0.is_null(index)
}

#[rustler::nif]
fn array_null_count(arr: ArrayResource) -> usize {
    arr.reference.0.null_count()
}

#[rustler::nif]
fn array_debug(arr: ArrayResource) -> String {
    let debug_string = format!("{:?}", arr.reference.0);
    debug_string
}
