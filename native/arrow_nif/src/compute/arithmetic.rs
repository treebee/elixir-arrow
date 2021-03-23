use crate::array::{ArrayResource, XArrayRef};
use crate::impl_compute_func;
use arrow::array::{
    ArrayRef, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::DataType;
use rustler::ResourceArc;
use std::sync::Arc;

impl_compute_func!(array_compute_add, add, arithmetic);
impl_compute_func!(array_compute_divide, divide, arithmetic);
impl_compute_func!(array_compute_multiply, multiply, arithmetic);
impl_compute_func!(array_compute_subtract, subtract, arithmetic);
