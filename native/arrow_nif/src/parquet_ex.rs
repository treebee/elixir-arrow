use crate::table::TableResource;
use rustler::ResourceArc;

#[rustler::nif]
fn read_table_parquet(path: String, columns: Vec<String>) -> ResourceArc<TableResource> {
    use parquet::arrow::{ArrowReader, ParquetFileArrowReader};
    use parquet::file::reader::SerializedFileReader;
    use std::fs::File;
    use std::sync::Arc;

    let file = File::open(path).unwrap();
    let file_reader = SerializedFileReader::new(file).unwrap();
    let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));

    let mut record_batch_reader = if columns.is_empty() {
        // TODO make sure to read the whole table
        arrow_reader.get_record_reader(2048).unwrap()
    } else {
        let schema = arrow_reader.get_schema().unwrap();
        let mut column_ids = Vec::new();
        for column in columns {
            for (i, field) in schema.fields().iter().enumerate() {
                if field.name() == &column {
                    column_ids.push(i);
                }
            }
        }
        arrow_reader
            .get_record_reader_by_columns(column_ids, 2048)
            .unwrap()
    };
    let record_batch = record_batch_reader.next().unwrap().unwrap();
    ResourceArc::new(TableResource(record_batch))
}
