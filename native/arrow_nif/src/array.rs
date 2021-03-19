use crate::datatype::XDataType;
use arrow::array::Int16Array;
use arrow::array::Int8Array;
use arrow::array::UInt16Array;
use arrow::array::UInt64Array;
use arrow::array::UInt8Array;
use arrow::array::{Float32Array, Float64Array, Int32Array, Int64Array, StringArray, UInt32Array};
use arrow::datatypes::DataType;
use rustler::Env;
use rustler::Term;
use rustler::{Encoder, ResourceArc};

pub struct Int8ArrayResource(Int8Array);
pub struct Int16ArrayResource(Int16Array);
pub struct Int32ArrayResource(Int32Array);
pub struct Int64ArrayResource(Int64Array);
pub struct UInt8ArrayResource(UInt8Array);
pub struct UInt16ArrayResource(UInt16Array);
pub struct UInt32ArrayResource(UInt32Array);
pub struct UInt64ArrayResource(UInt64Array);
pub struct Float32ArrayResource(Float32Array);
pub struct Float64ArrayResource(Float64Array);
pub struct StringArrayResource(StringArray);

// TODO this + the necessary constructs in the functions below look a bit strange
// the enum is used to support all necessary Array return types, but maybe that can be
// simplified somehow
pub enum ArrayResource {
    Int8(ResourceArc<Int8ArrayResource>),
    Int16(ResourceArc<Int16ArrayResource>),
    Int32(ResourceArc<Int32ArrayResource>),
    Int64(ResourceArc<Int64ArrayResource>),
    UInt8(ResourceArc<UInt8ArrayResource>),
    UInt16(ResourceArc<UInt16ArrayResource>),
    UInt32(ResourceArc<UInt32ArrayResource>),
    UInt64(ResourceArc<UInt64ArrayResource>),
    Float32(ResourceArc<Float32ArrayResource>),
    Float64(ResourceArc<Float64ArrayResource>),
    Utf8(ResourceArc<StringArrayResource>),
}

impl Encoder for ArrayResource {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        match self {
            ArrayResource::Int8(inner) => inner.encode(env),
            ArrayResource::Int16(inner) => inner.encode(env),
            ArrayResource::Int32(inner) => inner.encode(env),
            ArrayResource::Int64(inner) => inner.encode(env),
            ArrayResource::UInt8(inner) => inner.encode(env),
            ArrayResource::UInt16(inner) => inner.encode(env),
            ArrayResource::UInt32(inner) => inner.encode(env),
            ArrayResource::UInt64(inner) => inner.encode(env),
            ArrayResource::Float32(inner) => inner.encode(env),
            ArrayResource::Float64(inner) => inner.encode(env),
            ArrayResource::Utf8(inner) => inner.encode(env),
        }
    }
}

pub enum PrimitiveValue {
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
}

pub enum ArrayValues {
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
        }
    }
}

impl Encoder for ArrayValues {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        match self {
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
        }
    }
}

impl Int8ArrayResource {
    pub fn new(data: Vec<Option<i8>>) -> Int8ArrayResource {
        Int8ArrayResource(Int8Array::from(data))
    }
}

impl Int16ArrayResource {
    pub fn new(data: Vec<Option<i16>>) -> Int16ArrayResource {
        Int16ArrayResource(Int16Array::from(data))
    }
}

impl Int32ArrayResource {
    pub fn new(data: Vec<Option<i32>>) -> Int32ArrayResource {
        Int32ArrayResource(Int32Array::from(data))
    }
}

impl Int64ArrayResource {
    pub fn new(data: Vec<Option<i64>>) -> Int64ArrayResource {
        Int64ArrayResource(Int64Array::from(data))
    }
}

impl UInt8ArrayResource {
    fn new(data: Vec<Option<u8>>) -> UInt8ArrayResource {
        UInt8ArrayResource(UInt8Array::from(data))
    }
}

impl UInt16ArrayResource {
    fn new(data: Vec<Option<u16>>) -> UInt16ArrayResource {
        UInt16ArrayResource(UInt16Array::from(data))
    }
}

impl UInt32ArrayResource {
    fn new(data: Vec<Option<u32>>) -> UInt32ArrayResource {
        UInt32ArrayResource(UInt32Array::from(data))
    }
}

impl UInt64ArrayResource {
    fn new(data: Vec<Option<u64>>) -> UInt64ArrayResource {
        UInt64ArrayResource(UInt64Array::from(data))
    }
}

impl Float64ArrayResource {
    fn new(data: Vec<Option<f64>>) -> Float64ArrayResource {
        Float64ArrayResource(Float64Array::from(data))
    }
}

impl Float32ArrayResource {
    fn new(data: Vec<Option<f32>>) -> Float32ArrayResource {
        Float32ArrayResource(Float32Array::from(data))
    }
}

impl StringArrayResource {
    fn new(data: Vec<Option<String>>) -> StringArrayResource {
        let values: Vec<&str> = data
            .iter()
            .map(|s| match s {
                Some(t) => t.as_str(),
                None => "",
            })
            .collect();
        StringArrayResource(StringArray::from(values))
    }
}
#[rustler::nif]
fn make_array(a: Term, b: XDataType) -> ArrayResource {
    match &b.0 {
        DataType::Int8 => {
            let values: Vec<Option<i8>> = a.decode().unwrap();
            ArrayResource::Int8(ResourceArc::new(Int8ArrayResource::new(values)))
        }
        DataType::Int16 => {
            let values: Vec<Option<i16>> = a.decode().unwrap();
            ArrayResource::Int16(ResourceArc::new(Int16ArrayResource::new(values)))
        }
        DataType::Int32 => {
            let values: Vec<Option<i32>> = a.decode().unwrap();
            ArrayResource::Int32(ResourceArc::new(Int32ArrayResource::new(values)))
        }
        DataType::Int64 => {
            let values: Vec<Option<i64>> = a.decode().unwrap();
            ArrayResource::Int64(ResourceArc::new(Int64ArrayResource::new(values)))
        }
        DataType::UInt8 => {
            let values: Vec<Option<u8>> = a.decode().unwrap();
            ArrayResource::UInt8(ResourceArc::new(UInt8ArrayResource::new(values)))
        }
        DataType::UInt16 => {
            let values: Vec<Option<u16>> = a.decode().unwrap();
            ArrayResource::UInt16(ResourceArc::new(UInt16ArrayResource::new(values)))
        }
        DataType::UInt32 => {
            let values: Vec<Option<u32>> = a.decode().unwrap();
            ArrayResource::UInt32(ResourceArc::new(UInt32ArrayResource::new(values)))
        }
        DataType::UInt64 => {
            let values: Vec<Option<u64>> = a.decode().unwrap();
            ArrayResource::UInt64(ResourceArc::new(UInt64ArrayResource::new(values)))
        }
        DataType::Float32 => {
            let values: Vec<Option<f32>> = a.decode().unwrap();
            ArrayResource::Float32(ResourceArc::new(Float32ArrayResource::new(values)))
        }
        DataType::Float64 => {
            let values: Vec<Option<f64>> = a.decode().unwrap();
            ArrayResource::Float64(ResourceArc::new(Float64ArrayResource::new(values)))
        }
        DataType::Utf8 => {
            let values: Vec<Option<String>> = a.decode().unwrap();
            ArrayResource::Utf8(ResourceArc::new(StringArrayResource::new(values)))
        }
        // TODO error handling
        _ => ArrayResource::Int64(ResourceArc::new(Int64ArrayResource::new(vec![]))),
    }
}

#[rustler::nif]
fn to_list(arr: Term, dtype: XDataType) -> ArrayValues {
    match &dtype.0 {
        DataType::Int8 => {
            let array: ResourceArc<Int8ArrayResource> = arr.decode().unwrap();
            ArrayValues::Int8(array.0.into_iter().collect())
        }
        DataType::Int16 => {
            let array: ResourceArc<Int16ArrayResource> = arr.decode().unwrap();
            ArrayValues::Int16(array.0.into_iter().collect())
        }
        DataType::Int32 => {
            let array: ResourceArc<Int32ArrayResource> = arr.decode().unwrap();
            ArrayValues::Int32(array.0.into_iter().collect())
        }
        DataType::Int64 => {
            let array: ResourceArc<Int64ArrayResource> = arr.decode().unwrap();
            ArrayValues::Int64(array.0.into_iter().collect())
        }
        DataType::UInt8 => {
            let array: ResourceArc<UInt8ArrayResource> = arr.decode().unwrap();
            ArrayValues::UInt8(array.0.into_iter().collect())
        }
        DataType::UInt16 => {
            let array: ResourceArc<UInt16ArrayResource> = arr.decode().unwrap();
            ArrayValues::UInt16(array.0.into_iter().collect())
        }
        DataType::UInt32 => {
            let array: ResourceArc<UInt32ArrayResource> = arr.decode().unwrap();
            ArrayValues::UInt32(array.0.into_iter().collect())
        }
        DataType::UInt64 => {
            let array: ResourceArc<UInt64ArrayResource> = arr.decode().unwrap();
            ArrayValues::UInt64(array.0.into_iter().collect())
        }
        DataType::Float32 => {
            let array: ResourceArc<Float32ArrayResource> = arr.decode().unwrap();
            ArrayValues::Float32(array.0.into_iter().collect())
        }
        DataType::Float64 => {
            let array: ResourceArc<Float64ArrayResource> = arr.decode().unwrap();
            ArrayValues::Float64(array.0.into_iter().collect())
        }
        DataType::Utf8 => {
            let array: ResourceArc<StringArrayResource> = arr.decode().unwrap();
            let mut values: Vec<Option<String>> = Vec::new();
            for value in array.0.into_iter() {
                match value {
                    Some(t) => values.push(Some(String::from(t))),
                    None => values.push(Some(String::from(""))),
                }
            }
            ArrayValues::Utf8(values)
        }
        // TODO error handling
        _ => ArrayValues::Int64(vec![]),
    }
}

#[rustler::nif]
fn sum(arr: Term, dtype: XDataType) -> PrimitiveValue {
    match &dtype.0 {
        DataType::Int8 => {
            let array: ResourceArc<Int8ArrayResource> = arr.decode().unwrap();
            PrimitiveValue::Int8(arrow::compute::kernels::aggregate::sum(&array.0).unwrap())
        }
        DataType::Int16 => {
            let array: ResourceArc<Int16ArrayResource> = arr.decode().unwrap();
            PrimitiveValue::Int16(arrow::compute::kernels::aggregate::sum(&array.0).unwrap())
        }
        DataType::Int32 => {
            let array: ResourceArc<Int32ArrayResource> = arr.decode().unwrap();
            PrimitiveValue::Int32(arrow::compute::kernels::aggregate::sum(&array.0).unwrap())
        }
        DataType::Int64 => {
            let array: ResourceArc<Int64ArrayResource> = arr.decode().unwrap();
            PrimitiveValue::Int64(arrow::compute::kernels::aggregate::sum(&array.0).unwrap())
        }
        DataType::UInt8 => {
            let array: ResourceArc<UInt8ArrayResource> = arr.decode().unwrap();
            PrimitiveValue::UInt8(arrow::compute::kernels::aggregate::sum(&array.0).unwrap())
        }
        DataType::UInt16 => {
            let array: ResourceArc<UInt16ArrayResource> = arr.decode().unwrap();
            PrimitiveValue::UInt16(arrow::compute::kernels::aggregate::sum(&array.0).unwrap())
        }
        DataType::UInt32 => {
            let array: ResourceArc<UInt32ArrayResource> = arr.decode().unwrap();
            PrimitiveValue::UInt32(arrow::compute::kernels::aggregate::sum(&array.0).unwrap())
        }
        DataType::UInt64 => {
            let array: ResourceArc<UInt64ArrayResource> = arr.decode().unwrap();
            PrimitiveValue::UInt64(arrow::compute::kernels::aggregate::sum(&array.0).unwrap())
        }
        DataType::Float32 => {
            let array: ResourceArc<Float32ArrayResource> = arr.decode().unwrap();
            PrimitiveValue::Float32(arrow::compute::kernels::aggregate::sum(&array.0).unwrap())
        }
        DataType::Float64 => {
            let array: ResourceArc<Float64ArrayResource> = arr.decode().unwrap();
            PrimitiveValue::Float64(arrow::compute::kernels::aggregate::sum(&array.0).unwrap())
        }
        DataType::Utf8 => {
            panic!("String not supported")
        }
        _ => PrimitiveValue::Int32(0),
    }
}

#[rustler::nif]
fn len(arr: Term, dtype: XDataType) -> usize {
    match &dtype.0 {
        DataType::Int8 => {
            let array: ResourceArc<Int8ArrayResource> = arr.decode().unwrap();
            array.0.len()
        }
        DataType::Int16 => {
            let array: ResourceArc<Int16ArrayResource> = arr.decode().unwrap();
            array.0.len()
        }
        DataType::Int32 => {
            let array: ResourceArc<Int32ArrayResource> = arr.decode().unwrap();
            array.0.len()
        }
        DataType::Int64 => {
            let array: ResourceArc<Int64ArrayResource> = arr.decode().unwrap();
            array.0.len()
        }
        DataType::UInt8 => {
            let array: ResourceArc<UInt8ArrayResource> = arr.decode().unwrap();
            array.0.len()
        }
        DataType::UInt16 => {
            let array: ResourceArc<UInt16ArrayResource> = arr.decode().unwrap();
            array.0.len()
        }
        DataType::UInt32 => {
            let array: ResourceArc<UInt32ArrayResource> = arr.decode().unwrap();
            array.0.len()
        }
        DataType::UInt64 => {
            let array: ResourceArc<UInt64ArrayResource> = arr.decode().unwrap();
            array.0.len()
        }
        DataType::Float32 => {
            let array: ResourceArc<Float32ArrayResource> = arr.decode().unwrap();
            array.0.len()
        }
        DataType::Float64 => {
            let array: ResourceArc<Float64ArrayResource> = arr.decode().unwrap();
            array.0.len()
        }
        DataType::Utf8 => {
            let array: ResourceArc<StringArrayResource> = arr.decode().unwrap();
            // array.0.len()
            // TODO ???
            0
        }
        _ => 0,
    }
}
