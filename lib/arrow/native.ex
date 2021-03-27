defmodule Arrow.Native do
  use Rustler, otp_app: :arrow, crate: "arrow_nif"

  alias Arrow.RecordBatch

  def array_debug(_arg), do: error()

  def array_data_type(_arg), do: error()

  def array_sum(_arg), do: error()
  def array_min(_arg), do: error()
  def array_max(_arg), do: error()

  def to_list(_arg), do: error()

  def len(_arg), do: error()

  def make_array(_arg, _type), do: error()

  def array_slice(_arg, _offset, _length), do: error()

  def array_offset(_arg), do: error()

  def array_is_empty(_arg), do: error()

  def array_is_null(_arg, _index), do: error()

  def array_is_valid(_arg, _index), do: error()

  def array_null_count(_arg), do: error()

  def array_compute_add(_left, _right), do: error()
  def array_compute_divide(_left, _right), do: error()
  def array_compute_multiply(_left, _right), do: error()
  def array_compute_subtract(_left, _right), do: error()

  def array_compute_eq(_left, _right), do: error()
  def array_compute_neq(_left, _right), do: error()
  def array_compute_gt(_left, _right), do: error()
  def array_compute_gt_eq(_left, _right), do: error()
  def array_compute_lt(_left, _right), do: error()
  def array_compute_lt_eq(_left, _right), do: error()

  def array_compute_eq_utf8(_left, _right), do: error()
  def array_compute_neq_utf8(_left, _right), do: error()
  def array_compute_gt_utf8(_left, _right), do: error()
  def array_compute_gt_eq_utf8(_left, _right), do: error()
  def array_compute_lt_utf8(_left, _right), do: error()
  def array_compute_lt_eq_utf8(_left, _right), do: error()
  def array_compute_like_utf8(_left, _right), do: error()
  def array_compute_nlike_utf8(_left, _right), do: error()
  def array_compute_eq_scalar(_left, _right), do: error()
  def array_compute_neq_scalar(_left, _right), do: error()
  def array_compute_gt_scalar(_left, _right), do: error()
  def array_compute_gt_eq_scalar(_left, _right), do: error()
  def array_compute_lt_scalar(_left, _right), do: error()
  def array_compute_lt_eq_scalar(_left, _right), do: error()

  def array_compute_eq_utf8_scalar(_left, _right), do: error()
  def array_compute_neq_utf8_scalar(_left, _right), do: error()
  def array_compute_gt_utf8_scalar(_left, _right), do: error()
  def array_compute_gt_eq_utf8_scalar(_left, _right), do: error()
  def array_compute_lt_utf8_scalar(_left, _right), do: error()
  def array_compute_lt_eq_utf8_scalar(_left, _right), do: error()
  def array_compute_like_utf8_scalar(_left, _right), do: error()
  def array_compute_nlike_utf8_scalar(_left, _right), do: error()

  def array_compute_length(_arg), do: error()
  def get_field(), do: error()

  def echo_field(_field), do: error()

  def get_schema(_record_batch), do: error()

  def echo_schema(_schema), do: error()

  def get_record_batch(_schema), do: error()

  def debug_record_batch(_record_batch), do: error()

  def make_record_batch(_schema, _columns), do: error()
  def record_batch_to_map(_record_batch), do: error()

  # parquet functions
  def parquet_reader(_path), do: error()
  def parquet_reader_arrow_schema(_reader), do: error()
  def parquet_schema(_reader), do: error()

  def iter_batches(reader, batch_size, columns) do
    Stream.resource(
      fn -> record_reader(reader, batch_size, columns) end,
      fn reader ->
        case next_batch(reader) do
          nil -> {:halt, reader}
          batch -> {[%RecordBatch{reference: batch}], reader}
        end
      end,
      fn _reader -> nil end
    )
  end

  def record_reader(_reader, _batch_size, _columns), do: error()
  def next_batch(_reader), do: error()
  def write_record_batches(_batches, _path), do: error()

  # datafusion functions
  def query_parquet(_path, _table_name, _query), do: error()
  def create_datafusion_execution_context(), do: error()
  def datafusion_execution_context_register_parquet(_ctx, _table_name, _path), do: error()
  def datafusion_execution_context_register_csv(_ctx, _table_name, _path), do: error()
  def datafusion_execute_sql(_ctx, _query), do: error()

  defp error(), do: :erlang.nif_error(:nif_not_loaded)
end
