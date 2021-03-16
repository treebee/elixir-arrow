use crate::array::{
    len, make_array, sum, to_list, ArrayResource, Float32ArrayResource, Float64ArrayResource,
    Int32ArrayResource, Int64ArrayResource, UInt32ArrayResource,
};
use crate::field::XField;
use arrow::datatypes::{DataType, Field};
use rustler::{Env, Term};

pub fn on_load(_env: Env) -> bool {
    true
}

#[rustler::nif]
fn get_field() -> XField {
    let f = Field::new("my_field", DataType::Int64, false);
    XField::from_arrow(f)
}

#[rustler::nif]
fn echo_field(field: XField) -> XField {
    let arrow_field = field.to_arrow();
    println!("field: {:?}", arrow_field);
    field
}

fn load(env: Env, _: Term) -> bool {
    rustler::resource!(UInt32ArrayResource, env);
    rustler::resource!(Int32ArrayResource, env);
    rustler::resource!(Int64ArrayResource, env);
    rustler::resource!(Float64ArrayResource, env);
    rustler::resource!(Float32ArrayResource, env);
    rustler::resource!(ArrayResource, env);
    on_load(env);
    true
}

rustler::init!(
    "Elixir.Arrow",
    [make_array, sum, len, to_list, get_field, echo_field],
    load = load
);

mod array;
mod field;
