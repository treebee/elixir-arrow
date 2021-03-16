use arrow::array::{Float32Array, Float64Array, Int32Array, Int64Array, UInt32Array};
use arrow::datatypes::DataType;
use rustler::Atom;
use rustler::Decoder;
use rustler::Env;
use rustler::NifResult;
use rustler::Term;
use rustler::{Encoder, ResourceArc};

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

pub struct Int32ArrayResource(Int32Array);
pub struct Int64ArrayResource(Int64Array);
pub struct UInt32ArrayResource(UInt32Array);
pub struct Float64ArrayResource(Float64Array);
pub struct Float32ArrayResource(Float32Array);

pub enum ArrayResource {
    Int32(ResourceArc<Int32ArrayResource>),
    Int64(ResourceArc<Int64ArrayResource>),
    UInt32(ResourceArc<UInt32ArrayResource>),
    Float32(ResourceArc<Float32ArrayResource>),
    Float64(ResourceArc<Float64ArrayResource>),
}

impl Encoder for ArrayResource {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        match self {
            ArrayResource::Int32(inner) => inner.encode(env),
            ArrayResource::Int64(inner) => inner.encode(env),
            ArrayResource::UInt32(inner) => inner.encode(env),
            ArrayResource::Float32(inner) => inner.encode(env),
            ArrayResource::Float64(inner) => inner.encode(env),
        }
    }
}

pub enum XDataType {
    Int32,
    Int64,
    UInt32,
    Float32,
    Float64,
}

impl XDataType {
    pub fn from_arrow(data_type: &DataType) -> Self {
        match data_type {
            &DataType::Int32 => XDataType::Int32,
            &DataType::Int64 => XDataType::Int64,
            &DataType::Float32 => XDataType::Float32,
            &DataType::Float64 => XDataType::Float64,
            &DataType::UInt32 => XDataType::UInt32,
            _ => XDataType::Float64,
        }
    }
    pub fn to_arrow(&self) -> DataType {
        match self {
            XDataType::Int32 => DataType::Int32,
            XDataType::Int64 => DataType::Int64,
            XDataType::Float32 => DataType::Float32,
            XDataType::Float64 => DataType::Float64,
            XDataType::UInt32 => DataType::UInt32,
        }
    }
}

pub enum PrimitiveValue {
    Int32(i32),
    Int64(i64),
    UInt32(u32),
    Float32(f32),
    Float64(f64),
}

pub enum ArrayValues {
    Int32(Vec<i32>),
    Int64(Vec<i64>),
    UInt32(Vec<u32>),
    Float32(Vec<f32>),
    Float64(Vec<f64>),
}

impl Encoder for XDataType {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        match self {
            XDataType::Int32 => (atoms::s(), 32).encode(env),
            XDataType::Int64 => (atoms::s(), 64).encode(env),
            XDataType::UInt32 => (atoms::u(), 32).encode(env),
            XDataType::Float32 => (atoms::f(), 32).encode(env),
            XDataType::Float64 => (atoms::f(), 64).encode(env),
        }
    }
}

impl Encoder for PrimitiveValue {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        match self {
            PrimitiveValue::Int32(v) => v.encode(env),
            PrimitiveValue::Int64(v) => v.encode(env),
            PrimitiveValue::UInt32(v) => v.encode(env),
            PrimitiveValue::Float32(v) => v.encode(env),
            PrimitiveValue::Float64(v) => v.encode(env),
        }
    }
}

impl Encoder for ArrayValues {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        match self {
            ArrayValues::Int32(v) => v.encode(env),
            ArrayValues::Int64(v) => v.encode(env),
            ArrayValues::UInt32(v) => v.encode(env),
            ArrayValues::Float32(v) => v.encode(env),
            ArrayValues::Float64(v) => v.encode(env),
        }
    }
}

impl Int32ArrayResource {
    pub fn new(data: Vec<i32>) -> Int32ArrayResource {
        Int32ArrayResource(Int32Array::from(data))
    }
}

impl Int64ArrayResource {
    pub fn new(data: Vec<i64>) -> Int64ArrayResource {
        Int64ArrayResource(Int64Array::from(data))
    }
}

impl UInt32ArrayResource {
    fn new(data: Vec<u32>) -> UInt32ArrayResource {
        UInt32ArrayResource(UInt32Array::from(data))
    }
}

impl Float64ArrayResource {
    fn new(data: Vec<f64>) -> Float64ArrayResource {
        Float64ArrayResource(Float64Array::from(data))
    }
}

impl Float32ArrayResource {
    fn new(data: Vec<f32>) -> Float32ArrayResource {
        Float32ArrayResource(Float32Array::from(data))
    }
}

#[rustler::nif]
fn make_array(a: Term, b: Term) -> ArrayResource {
    let dtype = convert_to_datatype(b).unwrap();
    match dtype {
        XDataType::Int32 => {
            let values: Vec<i32> = a.decode().unwrap();
            ArrayResource::Int32(ResourceArc::new(Int32ArrayResource::new(values)))
        }
        XDataType::Int64 => {
            let values: Vec<i64> = a.decode().unwrap();
            ArrayResource::Int64(ResourceArc::new(Int64ArrayResource::new(values)))
        }
        XDataType::UInt32 => {
            let values: Vec<u32> = a.decode().unwrap();
            ArrayResource::UInt32(ResourceArc::new(UInt32ArrayResource::new(values)))
        }
        XDataType::Float32 => {
            let values: Vec<f32> = a.decode().unwrap();
            ArrayResource::Float32(ResourceArc::new(Float32ArrayResource::new(values)))
        }
        XDataType::Float64 => {
            let values: Vec<f64> = a.decode().unwrap();
            ArrayResource::Float64(ResourceArc::new(Float64ArrayResource::new(values)))
        }
    }
}

#[rustler::nif]
fn to_list(arr: Term, data_type: Term) -> ArrayValues {
    let dtype = convert_to_datatype(data_type).unwrap();
    match dtype {
        XDataType::Int32 => {
            let array: ResourceArc<Int32ArrayResource> = arr.decode().unwrap();
            ArrayValues::Int32(array.0.values().to_vec())
        }
        XDataType::Int64 => {
            let array: ResourceArc<Int64ArrayResource> = arr.decode().unwrap();
            ArrayValues::Int64(array.0.values().to_vec())
        }
        XDataType::UInt32 => {
            let array: ResourceArc<UInt32ArrayResource> = arr.decode().unwrap();
            ArrayValues::UInt32(array.0.values().to_vec())
        }
        XDataType::Float32 => {
            let array: ResourceArc<Float32ArrayResource> = arr.decode().unwrap();
            ArrayValues::Float32(array.0.values().to_vec())
        }
        XDataType::Float64 => {
            let array: ResourceArc<Float64ArrayResource> = arr.decode().unwrap();
            ArrayValues::Float64(array.0.values().to_vec())
        }
    }
}

#[rustler::nif]
fn sum(arr: Term, data_type: Term) -> PrimitiveValue {
    let dtype = convert_to_datatype(data_type).unwrap();
    match dtype {
        XDataType::Int32 => {
            let array: ResourceArc<Int32ArrayResource> = arr.decode().unwrap();
            PrimitiveValue::Int32(arrow::compute::kernels::aggregate::sum(&array.0).unwrap())
        }
        XDataType::Int64 => {
            let array: ResourceArc<Int64ArrayResource> = arr.decode().unwrap();
            PrimitiveValue::Int64(arrow::compute::kernels::aggregate::sum(&array.0).unwrap())
        }
        XDataType::UInt32 => {
            let array: ResourceArc<UInt32ArrayResource> = arr.decode().unwrap();
            PrimitiveValue::UInt32(arrow::compute::kernels::aggregate::sum(&array.0).unwrap())
        }
        XDataType::Float32 => {
            let array: ResourceArc<Float32ArrayResource> = arr.decode().unwrap();
            PrimitiveValue::Float32(arrow::compute::kernels::aggregate::sum(&array.0).unwrap())
        }
        XDataType::Float64 => {
            let array: ResourceArc<Float64ArrayResource> = arr.decode().unwrap();
            PrimitiveValue::Float64(arrow::compute::kernels::aggregate::sum(&array.0).unwrap())
        }
    }
}

#[rustler::nif]
fn len(arr: Term, data_type: Term) -> usize {
    let dtype = convert_to_datatype(data_type).unwrap();
    match dtype {
        XDataType::Int32 => {
            let array: ResourceArc<Int32ArrayResource> = arr.decode().unwrap();
            array.0.len()
        }
        XDataType::Int64 => {
            let array: ResourceArc<Int64ArrayResource> = arr.decode().unwrap();
            array.0.len()
        }
        XDataType::UInt32 => {
            let array: ResourceArc<UInt32ArrayResource> = arr.decode().unwrap();
            array.0.len()
        }
        XDataType::Float32 => {
            let array: ResourceArc<Float32ArrayResource> = arr.decode().unwrap();
            array.0.len()
        }
        XDataType::Float64 => {
            let array: ResourceArc<Float64ArrayResource> = arr.decode().unwrap();
            array.0.len()
        }
    }
}

fn convert_to_datatype(term: Term) -> Option<XDataType> {
    let (t, s): (Atom, usize) = term.decode().unwrap();
    if t == atoms::s() {
        match s {
            32 => Some(XDataType::Int32),
            64 => Some(XDataType::Int64),
            _ => None,
        }
    } else if t == atoms::f() {
        match s {
            32 => Some(XDataType::Float32),
            64 => Some(XDataType::Float64),
            _ => None,
        }
    } else if t == atoms::u() {
        match s {
            32 => Some(XDataType::UInt32),
            _ => None,
        }
    } else {
        None
    }
}

impl<'a> Decoder<'a> for XDataType {
    fn decode(t: Term<'a>) -> NifResult<Self> {
        Ok(convert_to_datatype(t).unwrap())
    }
}
