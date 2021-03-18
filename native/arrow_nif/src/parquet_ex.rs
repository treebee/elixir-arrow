use crate::table::RecordBatchResource;
use crate::XSchema;
use parquet::arrow::{ArrowReader, ParquetFileArrowReader};
use parquet::file::reader::{FileReader, SerializedFileReader};
use rustler::ResourceArc;
use std::fs::File;
use std::sync::Arc;

pub struct ParquetReaderResource(Arc<SerializedFileReader<File>>);
pub struct RecordBatchesResource(Vec<RecordBatchResource>);

#[rustler::nif]
fn read_table_parquet(path: String, columns: Vec<String>) -> ResourceArc<RecordBatchResource> {
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
    ResourceArc::new(RecordBatchResource(record_batch))
}

#[rustler::nif]
fn parquet_reader(path: String) -> ResourceArc<ParquetReaderResource> {
    let file = File::open(path).unwrap();
    let file_reader = SerializedFileReader::new(file).unwrap();
    ResourceArc::new(ParquetReaderResource(Arc::new(file_reader)))
}

#[rustler::nif]
fn parquet_reader_arrow_schema<'a>(reader: ResourceArc<ParquetReaderResource>) -> XSchema {
    let r = reader.0.clone();
    let mut arrow_reader = ParquetFileArrowReader::new(r);
    let schema = arrow_reader.get_schema().unwrap();
    XSchema::from_arrow(&schema)
}

#[rustler::nif]
fn parquet_schema(reader: ResourceArc<ParquetReaderResource>) {
    let r = reader.0.clone();
    println!("{:?}", r.metadata());
}

#[rustler::nif]
fn iter_batches(
    reader: ResourceArc<ParquetReaderResource>,
    batch_size: usize,
    columns: Vec<String>,
) -> Vec<ResourceArc<RecordBatchResource>> {
    let r = reader.0.clone();
    let mut arrow_reader = ParquetFileArrowReader::new(r);
    let record_batch_reader = if columns.is_empty() {
        arrow_reader.get_record_reader(batch_size).unwrap()
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
            .get_record_reader_by_columns(column_ids, batch_size)
            .unwrap()
    };
    let mut batches: Vec<ResourceArc<RecordBatchResource>> = Vec::new();
    for batch in record_batch_reader {
        let record_batch = batch.unwrap();
        if record_batch.num_rows() > 0 {
            batches.push(ResourceArc::new(RecordBatchResource(record_batch)));
        } else {
            break;
        }
    }
    batches
}
