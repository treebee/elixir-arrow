use crate::table::TableResource;
use rustler::ResourceArc;

#[rustler::nif]
fn read_table_parquet(path: String) -> ResourceArc<TableResource> {
    use parquet::arrow::{ArrowReader, ParquetFileArrowReader};
    use parquet::file::reader::SerializedFileReader;
    use std::fs::File;
    use std::sync::Arc;

    let file = File::open(path).unwrap();
    let file_reader = SerializedFileReader::new(file).unwrap();
    let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));

    // TODO make sure to read the whole table
    let mut record_batch_reader = arrow_reader.get_record_reader(2048).unwrap();
    let record_batch = record_batch_reader.next().unwrap().unwrap();
    ResourceArc::new(TableResource(record_batch))
}
