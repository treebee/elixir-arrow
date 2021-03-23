use crate::array::ArrayResource;
use crate::XArrayRef;
use arrow::array::{
    ArrayRef, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::DataType;
use rustler::ResourceArc;
use std::sync::Arc;

macro_rules! impl_func {
    ($nif:ident, $target:ident) => {
        #[rustler::nif]
        pub fn $nif(left: ArrayResource, right: ArrayResource) -> ArrayResource {
            assert_eq!(left.reference.0.data_type(), right.reference.0.data_type());
            match &left.reference.0.data_type() {
                DataType::Int8 => {
                    let res = arrow::compute::kernels::arithmetic::$target(
                        left.reference
                            .0
                            .as_any()
                            .downcast_ref::<Int8Array>()
                            .unwrap(),
                        right
                            .reference
                            .0
                            .as_any()
                            .downcast_ref::<Int8Array>()
                            .unwrap(),
                    )
                    .unwrap();
                    ArrayResource {
                        reference: ResourceArc::new(XArrayRef(Arc::new(res) as ArrayRef)),
                    }
                }
                DataType::Int16 => {
                    let res = arrow::compute::kernels::arithmetic::$target(
                        left.reference
                            .0
                            .as_any()
                            .downcast_ref::<Int16Array>()
                            .unwrap(),
                        right
                            .reference
                            .0
                            .as_any()
                            .downcast_ref::<Int16Array>()
                            .unwrap(),
                    )
                    .unwrap();
                    ArrayResource {
                        reference: ResourceArc::new(XArrayRef(Arc::new(res) as ArrayRef)),
                    }
                }
                DataType::Int32 => {
                    let res = arrow::compute::kernels::arithmetic::$target(
                        left.reference
                            .0
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .unwrap(),
                        right
                            .reference
                            .0
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .unwrap(),
                    )
                    .unwrap();
                    ArrayResource {
                        reference: ResourceArc::new(XArrayRef(Arc::new(res) as ArrayRef)),
                    }
                }
                DataType::Int64 => {
                    let res = arrow::compute::kernels::arithmetic::$target(
                        left.reference
                            .0
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap(),
                        right
                            .reference
                            .0
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap(),
                    )
                    .unwrap();
                    ArrayResource {
                        reference: ResourceArc::new(XArrayRef(Arc::new(res) as ArrayRef)),
                    }
                }
                DataType::UInt8 => {
                    let res = arrow::compute::kernels::arithmetic::$target(
                        left.reference
                            .0
                            .as_any()
                            .downcast_ref::<UInt8Array>()
                            .unwrap(),
                        right
                            .reference
                            .0
                            .as_any()
                            .downcast_ref::<UInt8Array>()
                            .unwrap(),
                    )
                    .unwrap();
                    ArrayResource {
                        reference: ResourceArc::new(XArrayRef(Arc::new(res) as ArrayRef)),
                    }
                }
                DataType::UInt16 => {
                    let res = arrow::compute::kernels::arithmetic::$target(
                        left.reference
                            .0
                            .as_any()
                            .downcast_ref::<UInt16Array>()
                            .unwrap(),
                        right
                            .reference
                            .0
                            .as_any()
                            .downcast_ref::<UInt16Array>()
                            .unwrap(),
                    )
                    .unwrap();
                    ArrayResource {
                        reference: ResourceArc::new(XArrayRef(Arc::new(res) as ArrayRef)),
                    }
                }
                DataType::UInt32 => {
                    let res = arrow::compute::kernels::arithmetic::$target(
                        left.reference
                            .0
                            .as_any()
                            .downcast_ref::<UInt32Array>()
                            .unwrap(),
                        right
                            .reference
                            .0
                            .as_any()
                            .downcast_ref::<UInt32Array>()
                            .unwrap(),
                    )
                    .unwrap();
                    ArrayResource {
                        reference: ResourceArc::new(XArrayRef(Arc::new(res) as ArrayRef)),
                    }
                }
                DataType::UInt64 => {
                    let res = arrow::compute::kernels::arithmetic::$target(
                        left.reference
                            .0
                            .as_any()
                            .downcast_ref::<UInt64Array>()
                            .unwrap(),
                        right
                            .reference
                            .0
                            .as_any()
                            .downcast_ref::<UInt64Array>()
                            .unwrap(),
                    )
                    .unwrap();
                    ArrayResource {
                        reference: ResourceArc::new(XArrayRef(Arc::new(res) as ArrayRef)),
                    }
                }
                DataType::Float32 => {
                    let res = arrow::compute::kernels::arithmetic::$target(
                        left.reference
                            .0
                            .as_any()
                            .downcast_ref::<Float32Array>()
                            .unwrap(),
                        right
                            .reference
                            .0
                            .as_any()
                            .downcast_ref::<Float32Array>()
                            .unwrap(),
                    )
                    .unwrap();
                    ArrayResource {
                        reference: ResourceArc::new(XArrayRef(Arc::new(res) as ArrayRef)),
                    }
                }
                DataType::Float64 => {
                    let res = arrow::compute::kernels::arithmetic::$target(
                        left.reference
                            .0
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .unwrap(),
                        right
                            .reference
                            .0
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .unwrap(),
                    )
                    .unwrap();
                    ArrayResource {
                        reference: ResourceArc::new(XArrayRef(Arc::new(res) as ArrayRef)),
                    }
                }
                dtype => panic!("datatype {} is not supported", dtype),
            }
        }
    };
}

impl_func!(array_compute_add, add);
impl_func!(array_compute_divide, divide);
impl_func!(array_compute_multiply, multiply);
impl_func!(array_compute_subtract, subtract);
