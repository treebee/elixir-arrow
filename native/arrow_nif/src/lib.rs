use crate::array::{
    len, make_array, sum, to_list, ArrayResource, Float32ArrayResource, Float64ArrayResource,
    Int32ArrayResource, Int64ArrayResource, UInt32ArrayResource,
};
use rustler::{Env, Term};

pub fn on_load(_env: Env) -> bool {
    true
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

rustler::init!("Elixir.Arrow", [make_array, sum, len, to_list], load = load);

mod array;
