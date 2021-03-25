use crate::array::{ArrayResource, PrimitiveValue};
use arrow::array::{
    BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::DataType;

#[macro_export]
macro_rules! impl_aggregate_func {
    ($nif:ident, $target:ident, $booltarget:ident, $stringtarget:ident) => {
        #[rustler::nif]
        pub fn $nif(arr: ArrayResource) -> PrimitiveValue {
            let array = &arr.reference.0;
            match arr.reference.0.data_type() {
                DataType::Int8 => PrimitiveValue::Int8(
                    arrow::compute::kernels::aggregate::$target(
                        array.as_any().downcast_ref::<Int8Array>().unwrap(),
                    )
                    .unwrap(),
                ),
                DataType::Int16 => PrimitiveValue::Int16(
                    arrow::compute::kernels::aggregate::$target(
                        array.as_any().downcast_ref::<Int16Array>().unwrap(),
                    )
                    .unwrap(),
                ),
                DataType::Int32 => PrimitiveValue::Int32(
                    arrow::compute::kernels::aggregate::$target(
                        array.as_any().downcast_ref::<Int32Array>().unwrap(),
                    )
                    .unwrap(),
                ),
                DataType::Int64 => PrimitiveValue::Int64(
                    arrow::compute::kernels::aggregate::$target(
                        array.as_any().downcast_ref::<Int64Array>().unwrap(),
                    )
                    .unwrap(),
                ),
                DataType::UInt8 => PrimitiveValue::UInt8(
                    arrow::compute::kernels::aggregate::$target(
                        array.as_any().downcast_ref::<UInt8Array>().unwrap(),
                    )
                    .unwrap(),
                ),
                DataType::UInt16 => PrimitiveValue::UInt16(
                    arrow::compute::kernels::aggregate::$target(
                        array.as_any().downcast_ref::<UInt16Array>().unwrap(),
                    )
                    .unwrap(),
                ),
                DataType::UInt32 => PrimitiveValue::UInt32(
                    arrow::compute::kernels::aggregate::$target(
                        array.as_any().downcast_ref::<UInt32Array>().unwrap(),
                    )
                    .unwrap(),
                ),
                DataType::UInt64 => PrimitiveValue::UInt64(
                    arrow::compute::kernels::aggregate::$target(
                        array.as_any().downcast_ref::<UInt64Array>().unwrap(),
                    )
                    .unwrap(),
                ),
                DataType::Float32 => PrimitiveValue::Float32(
                    arrow::compute::kernels::aggregate::$target(
                        array.as_any().downcast_ref::<Float32Array>().unwrap(),
                    )
                    .unwrap(),
                ),
                DataType::Float64 => PrimitiveValue::Float64(
                    arrow::compute::kernels::aggregate::$target(
                        array.as_any().downcast_ref::<Float64Array>().unwrap(),
                    )
                    .unwrap(),
                ),
                DataType::Boolean => PrimitiveValue::Boolean(
                    arrow::compute::kernels::aggregate::$booltarget(
                        array.as_any().downcast_ref::<BooleanArray>().unwrap(),
                    )
                    .unwrap(),
                ),
                DataType::Utf8 => PrimitiveValue::Utf8(
                    match arrow::compute::kernels::aggregate::$stringtarget(
                        array.as_any().downcast_ref::<StringArray>().unwrap(),
                    ) {
                        Some(s) => Some(String::from(s)),
                        None => None,
                    },
                ),
                dtype => panic!("aggregate function not implemented for {}", dtype),
            }
        }
    };
}

impl_aggregate_func!(array_min, min, min_boolean, min_string);
impl_aggregate_func!(array_max, max, max_boolean, max_string);

#[rustler::nif]
pub fn array_sum(arr: ArrayResource) -> PrimitiveValue {
    let array = &arr.reference.0;
    match arr.reference.0.data_type() {
        DataType::Int8 => PrimitiveValue::Int8(
            arrow::compute::kernels::aggregate::sum(
                array.as_any().downcast_ref::<Int8Array>().unwrap(),
            )
            .unwrap(),
        ),
        DataType::Int16 => PrimitiveValue::Int16(
            arrow::compute::kernels::aggregate::sum(
                array.as_any().downcast_ref::<Int16Array>().unwrap(),
            )
            .unwrap(),
        ),
        DataType::Int32 => PrimitiveValue::Int32(
            arrow::compute::kernels::aggregate::sum(
                array.as_any().downcast_ref::<Int32Array>().unwrap(),
            )
            .unwrap(),
        ),
        DataType::Int64 => PrimitiveValue::Int64(
            arrow::compute::kernels::aggregate::sum(
                array.as_any().downcast_ref::<Int64Array>().unwrap(),
            )
            .unwrap(),
        ),
        DataType::UInt8 => PrimitiveValue::UInt8(
            arrow::compute::kernels::aggregate::sum(
                array.as_any().downcast_ref::<UInt8Array>().unwrap(),
            )
            .unwrap(),
        ),
        DataType::UInt16 => PrimitiveValue::UInt16(
            arrow::compute::kernels::aggregate::sum(
                array.as_any().downcast_ref::<UInt16Array>().unwrap(),
            )
            .unwrap(),
        ),
        DataType::UInt32 => PrimitiveValue::UInt32(
            arrow::compute::kernels::aggregate::sum(
                array.as_any().downcast_ref::<UInt32Array>().unwrap(),
            )
            .unwrap(),
        ),
        DataType::UInt64 => PrimitiveValue::UInt64(
            arrow::compute::kernels::aggregate::sum(
                array.as_any().downcast_ref::<UInt64Array>().unwrap(),
            )
            .unwrap(),
        ),
        DataType::Float32 => PrimitiveValue::Float32(
            arrow::compute::kernels::aggregate::sum(
                array.as_any().downcast_ref::<Float32Array>().unwrap(),
            )
            .unwrap(),
        ),
        DataType::Float64 => PrimitiveValue::Float64(
            arrow::compute::kernels::aggregate::sum(
                array.as_any().downcast_ref::<Float64Array>().unwrap(),
            )
            .unwrap(),
        ),
        dtype => panic!("array.sum() not implemented for {}", dtype),
    }
}
