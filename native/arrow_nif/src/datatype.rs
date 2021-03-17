use crate::atoms;
use arrow::datatypes::DataType;
use rustler::{Atom, Decoder, Encoder, Env, NifResult, Term};

pub struct XDataType(pub DataType);

impl XDataType {
    pub fn from_arrow(data_type: &DataType) -> Self {
        let dtype = data_type.clone();
        XDataType(dtype)
    }
    pub fn to_arrow(&self) -> &DataType {
        &self.0
    }
}

impl Encoder for XDataType {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        match &self.0 {
            DataType::Int32 => (atoms::s(), 32).encode(env),
            DataType::Int64 => (atoms::s(), 64).encode(env),
            DataType::UInt32 => (atoms::u(), 32).encode(env),
            DataType::Float32 => (atoms::f(), 32).encode(env),
            DataType::Float64 => (atoms::f(), 64).encode(env),
            _ => (atoms::error(), 0).encode(env),
        }
    }
}

fn convert_to_datatype(term: Term) -> Option<XDataType> {
    let (t, s): (Atom, usize) = term.decode().unwrap();
    if t == atoms::s() {
        match s {
            32 => Some(XDataType(DataType::Int32)),
            64 => Some(XDataType(DataType::Int64)),
            _ => None,
        }
    } else if t == atoms::f() {
        match s {
            32 => Some(XDataType(DataType::Float32)),
            64 => Some(XDataType(DataType::Float64)),
            _ => None,
        }
    } else if t == atoms::u() {
        match s {
            32 => Some(XDataType(DataType::UInt32)),
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
