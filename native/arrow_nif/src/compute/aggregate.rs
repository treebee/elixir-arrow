use crate::array::{ArrayResource, PrimitiveValue};
use arrow::array::{
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::DataType;

#[rustler::nif]
pub fn sum(arr: ArrayResource) -> PrimitiveValue {
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
        DataType::Boolean => panic!("array sum not implemented for Boolean"),
        dtype => panic!("array sum not implemented for {}", dtype),
    }
}
