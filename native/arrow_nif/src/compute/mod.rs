pub mod arithmetic;
pub mod comparison;

#[macro_export]
macro_rules! impl_compute_func {
    ($nif:ident, $target:ident, $module:ident) => {
        #[rustler::nif]
        pub fn $nif(left: ArrayResource, right: ArrayResource) -> ArrayResource {
            assert_eq!(left.reference.0.data_type(), right.reference.0.data_type());
            match &left.reference.0.data_type() {
                DataType::Int8 => {
                    let res = arrow::compute::kernels::$module::$target(
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
                    let res = arrow::compute::kernels::$module::$target(
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
                    let res = arrow::compute::kernels::$module::$target(
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
                    let res = arrow::compute::kernels::$module::$target(
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
                    let res = arrow::compute::kernels::$module::$target(
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
                    let res = arrow::compute::kernels::$module::$target(
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
                    let res = arrow::compute::kernels::$module::$target(
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
                    let res = arrow::compute::kernels::$module::$target(
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
                    let res = arrow::compute::kernels::$module::$target(
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
                    let res = arrow::compute::kernels::$module::$target(
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

#[macro_export]
macro_rules! impl_compute_func_scalar {
    ($nif:ident, $target:ident, $module:ident) => {
        #[rustler::nif]
        pub fn $nif(left: ArrayResource, right: Term) -> ArrayResource {
            match &left.reference.0.data_type() {
                DataType::Int8 => {
                    let right: i8 = right.decode().unwrap();
                    let res = arrow::compute::kernels::$module::$target(
                        left.reference
                            .0
                            .as_any()
                            .downcast_ref::<Int8Array>()
                            .unwrap(),
                        right,
                    )
                    .unwrap();
                    ArrayResource {
                        reference: ResourceArc::new(XArrayRef(Arc::new(res) as ArrayRef)),
                    }
                }
                DataType::Int16 => {
                    let right: i16 = right.decode().unwrap();
                    let res = arrow::compute::kernels::$module::$target(
                        left.reference
                            .0
                            .as_any()
                            .downcast_ref::<Int16Array>()
                            .unwrap(),
                        right,
                    )
                    .unwrap();
                    ArrayResource {
                        reference: ResourceArc::new(XArrayRef(Arc::new(res) as ArrayRef)),
                    }
                }
                DataType::Int32 => {
                    let right: i32 = right.decode().unwrap();
                    let res = arrow::compute::kernels::$module::$target(
                        left.reference
                            .0
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .unwrap(),
                        right,
                    )
                    .unwrap();
                    ArrayResource {
                        reference: ResourceArc::new(XArrayRef(Arc::new(res) as ArrayRef)),
                    }
                }
                DataType::Int64 => {
                    let right: i64 = right.decode().unwrap();
                    let res = arrow::compute::kernels::$module::$target(
                        left.reference
                            .0
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap(),
                        right,
                    )
                    .unwrap();
                    ArrayResource {
                        reference: ResourceArc::new(XArrayRef(Arc::new(res) as ArrayRef)),
                    }
                }
                DataType::UInt8 => {
                    let right: u8 = right.decode().unwrap();
                    let res = arrow::compute::kernels::$module::$target(
                        left.reference
                            .0
                            .as_any()
                            .downcast_ref::<UInt8Array>()
                            .unwrap(),
                        right,
                    )
                    .unwrap();
                    ArrayResource {
                        reference: ResourceArc::new(XArrayRef(Arc::new(res) as ArrayRef)),
                    }
                }
                DataType::UInt16 => {
                    let right: u16 = right.decode().unwrap();
                    let res = arrow::compute::kernels::$module::$target(
                        left.reference
                            .0
                            .as_any()
                            .downcast_ref::<UInt16Array>()
                            .unwrap(),
                        right,
                    )
                    .unwrap();
                    ArrayResource {
                        reference: ResourceArc::new(XArrayRef(Arc::new(res) as ArrayRef)),
                    }
                }
                DataType::UInt32 => {
                    let right: u32 = right.decode().unwrap();
                    let res = arrow::compute::kernels::$module::$target(
                        left.reference
                            .0
                            .as_any()
                            .downcast_ref::<UInt32Array>()
                            .unwrap(),
                        right,
                    )
                    .unwrap();
                    ArrayResource {
                        reference: ResourceArc::new(XArrayRef(Arc::new(res) as ArrayRef)),
                    }
                }
                DataType::UInt64 => {
                    let right: u64 = right.decode().unwrap();
                    let res = arrow::compute::kernels::$module::$target(
                        left.reference
                            .0
                            .as_any()
                            .downcast_ref::<UInt64Array>()
                            .unwrap(),
                        right,
                    )
                    .unwrap();
                    ArrayResource {
                        reference: ResourceArc::new(XArrayRef(Arc::new(res) as ArrayRef)),
                    }
                }
                DataType::Float32 => {
                    let right: f32 = right.decode().unwrap();
                    let res = arrow::compute::kernels::$module::$target(
                        left.reference
                            .0
                            .as_any()
                            .downcast_ref::<Float32Array>()
                            .unwrap(),
                        right,
                    )
                    .unwrap();
                    ArrayResource {
                        reference: ResourceArc::new(XArrayRef(Arc::new(res) as ArrayRef)),
                    }
                }
                DataType::Float64 => {
                    let right: f64 = right.decode().unwrap();
                    let res = arrow::compute::kernels::$module::$target(
                        left.reference
                            .0
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .unwrap(),
                        right,
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
