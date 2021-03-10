use crate::array::{
    len, make_int64_array, make_uint32_array, sum, to_list, Int64ArrayResource, UInt32ArrayResource,
};
use rustler::Env;

pub fn on_load(_env: Env) -> bool {
    true
}

fn load(env: rustler::Env, _: rustler::Term) -> bool {
    rustler::resource!(UInt32ArrayResource, env);
    rustler::resource!(Int64ArrayResource, env);
    on_load(env);
    true
}

rustler::init!(
    "Elixir.Arrow",
    [make_uint32_array, make_int64_array, sum, len, to_list],
    load = load
);

mod array;
