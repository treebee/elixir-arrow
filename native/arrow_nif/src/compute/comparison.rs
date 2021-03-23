use crate::array::{ArrayResource, XArrayRef};
use crate::{impl_compute_func, impl_compute_func_scalar, impl_compute_func_utf8};
use arrow::array::{
    ArrayRef, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::DataType;
use rustler::{ResourceArc, Term};
use std::sync::Arc;

impl_compute_func!(array_compute_eq, eq, comparison);
impl_compute_func!(array_compute_neq, neq, comparison);
impl_compute_func!(array_compute_gt, gt, comparison);
impl_compute_func!(array_compute_gt_eq, gt_eq, comparison);
impl_compute_func!(array_compute_lt, lt, comparison);
impl_compute_func!(array_compute_lt_eq, lt_eq, comparison);

impl_compute_func_scalar!(array_compute_eq_scalar, eq_scalar, comparison);
impl_compute_func_scalar!(array_compute_neq_scalar, neq_scalar, comparison);
impl_compute_func_scalar!(array_compute_gt_scalar, gt_scalar, comparison);
impl_compute_func_scalar!(array_compute_gt_eq_scalar, gt_eq_scalar, comparison);
impl_compute_func_scalar!(array_compute_lt_scalar, lt_scalar, comparison);
impl_compute_func_scalar!(array_compute_lt_eq_scalar, lt_eq_scalar, comparison);

impl_compute_func_utf8!(array_compute_eq_utf8, eq_utf8, comparison);
impl_compute_func_utf8!(array_compute_neq_utf8, neq_utf8, comparison);
impl_compute_func_utf8!(array_compute_gt_utf8, gt_utf8, comparison);
impl_compute_func_utf8!(array_compute_gt_eq_utf8, gt_eq_utf8, comparison);
impl_compute_func_utf8!(array_compute_lt_utf8, lt_utf8, comparison);
impl_compute_func_utf8!(array_compute_lt_eq_utf8, lt_eq_utf8, comparison);
