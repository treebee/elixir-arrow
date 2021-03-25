use crate::array::{ArrayResource, XArrayRef};
use arrow::array::StringArray;
use rustler::ResourceArc;

#[rustler::nif]
pub fn array_compute_length(arr: ArrayResource) -> ArrayResource {
    let array = &arr.reference.0;
    ArrayResource {
        reference: ResourceArc::new(XArrayRef(
            arrow::compute::kernels::length::length(
                array.as_any().downcast_ref::<StringArray>().unwrap(),
            )
            .unwrap(),
        )),
    }
}
