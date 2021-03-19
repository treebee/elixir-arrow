use crate::schema::XSchema;
use crate::table::RecordBatchResource;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::arrow::{ArrowReader, ParquetFileArrowReader};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use rustler::ResourceArc;
use std::fs::File;
use std::sync::{Arc, Mutex};

pub struct ParquetReaderResource(Arc<SerializedFileReader<File>>);
pub struct ParquetRecordBatchReaderResource(Mutex<ParquetRecordBatchReader>);
pub struct RecordBatchesResource(Vec<RecordBatchResource>);

unsafe impl Send for ParquetRecordBatchReaderResource {}
unsafe impl Sync for ParquetRecordBatchReaderResource {}

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

// TODO: encoder for ParquetSchema
#[rustler::nif]
fn parquet_schema(reader: ResourceArc<ParquetReaderResource>) {
    let r = reader.0.clone();
    println!("{:?}", r.metadata());
}

fn create_record_reader(
    mut arrow_reader: ParquetFileArrowReader,
    batch_size: usize,
    columns: Vec<String>,
) -> ParquetRecordBatchReader {
    if columns.is_empty() {
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
    }
}

#[rustler::nif]
fn record_reader(
    reader: ResourceArc<ParquetReaderResource>,
    batch_size: usize,
    columns: Vec<String>,
) -> ResourceArc<ParquetRecordBatchReaderResource> {
    let r = reader.0.clone();
    let arrow_reader = ParquetFileArrowReader::new(r);
    let record_batch_reader = create_record_reader(arrow_reader, batch_size, columns);
    ResourceArc::new(ParquetRecordBatchReaderResource(Mutex::new(
        record_batch_reader,
    )))
}

#[rustler::nif]
fn next_batch(
    reader: ResourceArc<ParquetRecordBatchReaderResource>,
) -> Option<ResourceArc<RecordBatchResource>> {
    let mut r = reader.0.lock().unwrap();
    let record_batch = r.next();
    match record_batch {
        Some(batch) => Some(ResourceArc::new(RecordBatchResource(batch.unwrap()))),
        None => None,
    }
}

#[rustler::nif]
fn write_record_batches(path: String, batches: Vec<ResourceArc<RecordBatchResource>>) {
    let schema = batches[0].0.schema();
    let file = File::create(path).unwrap();
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
    for batch in &batches {
        writer.write(&batch.0).unwrap();
    }
    writer.close().unwrap();
}
