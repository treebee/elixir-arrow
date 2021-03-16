use crate::array::XDataType;
use arrow::datatypes::Field;
use rustler::NifStruct;

#[derive(NifStruct)]
#[module = "Arrow.Field"]
pub struct XField {
    name: String,
    data_type: XDataType,
    nullable: bool,
    dict_id: i64,
    dict_is_ordered: bool,
    // let's keep this None for now
    metadata: Option<String>,
}

impl XField {
    pub fn from_arrow(field: Field) -> Self {
        XField {
            name: String::from(field.name()),
            data_type: XDataType::from_arrow(field.data_type()),
            nullable: field.is_nullable(),
            // no dict support yet
            dict_id: 0,
            dict_is_ordered: false,
            // metadata is always None for now
            metadata: None,
        }
    }

    pub fn to_arrow(&self) -> Field {
        Field::new(self.name.as_str(), self.data_type.to_arrow(), self.nullable)
    }
}
