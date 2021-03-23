use crate::atoms;
use arrow::datatypes::DataType;
use rustler::{Atom, Decoder, Encoder, Env, NifResult, Term};

#[derive(Debug)]
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
            DataType::Boolean => (atoms::u(), 1).encode(env),
            DataType::Int8 => (atoms::s(), 8).encode(env),
            DataType::Int16 => (atoms::s(), 16).encode(env),
            DataType::Int32 => (atoms::s(), 32).encode(env),
            DataType::Int64 => (atoms::s(), 64).encode(env),
            DataType::UInt8 => (atoms::u(), 8).encode(env),
            DataType::UInt16 => (atoms::u(), 16).encode(env),
            DataType::UInt32 => (atoms::u(), 32).encode(env),
            DataType::UInt64 => (atoms::u(), 64).encode(env),
            DataType::Float32 => (atoms::f(), 32).encode(env),
            DataType::Float64 => (atoms::f(), 64).encode(env),
            DataType::Utf8 => (atoms::utf8(), 32).encode(env),
            _ => (atoms::error(), 0).encode(env),
        }
    }
}

fn convert_to_datatype(term: Term) -> Option<XDataType> {
    let (t, s): (Atom, usize) = term.decode().unwrap();
    if t == atoms::s() {
        match s {
            8 => Some(XDataType(DataType::Int8)),
            16 => Some(XDataType(DataType::Int16)),
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
            1 => Some(XDataType(DataType::Boolean)),
            8 => Some(XDataType(DataType::UInt8)),
            16 => Some(XDataType(DataType::UInt16)),
            32 => Some(XDataType(DataType::UInt32)),
            64 => Some(XDataType(DataType::UInt64)),
            _ => None,
        }
    } else if t == atoms::utf8() {
        match s {
            32 => Some(XDataType(DataType::Utf8)),
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
