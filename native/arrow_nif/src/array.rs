use arrow::array::{Int64Array, UInt32Array};
use rustler::ResourceArc;

// TODO(pmuehlbauer) is it possible to have this as generic types and functions?
pub struct Int64ArrayResource(Int64Array);
pub struct UInt32ArrayResource(UInt32Array);

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

#[rustler::nif]
fn make_int64_array(args: Vec<i64>) -> ResourceArc<Int64ArrayResource> {
    ResourceArc::new(Int64ArrayResource::new(args))
}

#[rustler::nif]
fn make_uint32_array(args: Vec<u32>) -> ResourceArc<UInt32ArrayResource> {
    ResourceArc::new(UInt32ArrayResource::new(args))
}

#[rustler::nif]
fn to_list(arr: ResourceArc<Int64ArrayResource>) -> Vec<i64> {
    arr.0.values().to_vec()
}

#[rustler::nif]
fn sum(arr: ResourceArc<Int64ArrayResource>) -> i64 {
    arrow::compute::kernels::aggregate::sum(&arr.0).unwrap()
}

#[rustler::nif]
fn len(arr: ResourceArc<Int64ArrayResource>) -> usize {
    arr.0.len()
}
