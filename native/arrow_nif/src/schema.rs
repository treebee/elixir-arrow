use crate::field::XField;
use arrow::datatypes::Schema;
use rustler::{Decoder, Encoder, Env, NifResult, NifStruct, Term};
use std::collections::HashMap;

pub struct MetaData(HashMap<String, String>);

impl MetaData {
    pub fn new() -> Self {
        Self(HashMap::new())
    }
}

#[derive(NifStruct)]
#[module = "Arrow.Schema"]
pub struct XSchema {
    pub fields: Vec<XField>,
    pub metadata: MetaData,
}

impl XSchema {
    pub fn to_arrow(&self) -> Schema {
        let metadata = self.metadata.0.clone();
        Schema::new_with_metadata(
            self.fields.iter().map(|field| field.to_arrow()).collect(),
            metadata,
        )
    }
}

impl<'a> Decoder<'a> for MetaData {
    fn decode(term: Term<'a>) -> NifResult<Self> {
        let data: Vec<(String, String)> = term.decode().unwrap();
        let mut metadata = MetaData(HashMap::new());
        for (key, val) in data.iter() {
            metadata.0.insert(String::from(key), String::from(val));
        }
        Ok(metadata)
    }
}

impl Encoder for MetaData {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        let map = self.0.clone();
        let mut m: Vec<(String, String)> = vec![];
        for (key, val) in map.into_iter() {
            m.push((key, val));
        }
        m.encode(env)
    }
}
