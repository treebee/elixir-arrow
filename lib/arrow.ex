defmodule Arrow do
  @moduledoc """
  Elixir bindings for [Apache Arrow](https://arrow.apache.org/).

  This is currently an experimental library to bring Arrow and its ecosystem
  to Elixir. It's very early work in progress.

  It uses the Rust implementation via [rustler](https://github.com/rusterlium/rustler/).

  NOTE:
  It is not an offical library of the Apache Arrow project!

  In addition to Arrow, the library comes with support for
  reading and writing [Parquet](https://parquet.apache.org/) files.

  """
  use Rustler, otp_app: :arrow, crate: "arrow_nif"

  alias Arrow.RecordBatch

  @doc """
  Creates an Arrow array.

  For example to create an integer array:

      iex> arr = Arrow.array([1, 5, 3, nil, 6])
      #Arrow.Int64Array
      [1, 5, 3, nil, 6]

  Here, the type was inferred for us, but it is also possible to
  provide one explicitly:

      iex> arr = Arrow.array([1, 5, 3, nil, 6], type: {:f, 32})
      #Arrow.Float32Array
      [1.0, 5.0, 3.0, nil, 6.0]


  """
  @doc type: :creation
  def array(arg, opts \\ [])
  # arrow functions
  def array(arg, opts) do
    type = Arrow.Type.normalize!(opts[:type] || Arrow.Type.infer(arg))

    arr =
      case type do
        {:f, _} -> Enum.map(arg, &to_float/1)
        {:u, 1} -> Enum.map(arg, &to_bool/1)
        {:u, _} -> Enum.map(arg, &to_int/1)
        {:s, _} -> Enum.map(arg, &to_int/1)
        _ -> arg
      end

    make_array(arr, type)
  end

  def array_data_type(_arg), do: error()

  def sum(_arg), do: error()

  def to_list(_arg), do: error()

  def len(_arg), do: error()

  def make_array(_arg, _type), do: error()

  def array_slice(_arg, _offset, _length), do: error()

  def array_offset(_arg), do: error()

  def array_is_empty(_arg), do: error()

  def array_is_null(_arg, _index), do: error()

  def array_is_valid(_arg, _index), do: error()

  def array_null_count(_arg), do: error()

  def get_field(), do: error()

  def echo_field(_field), do: error()

  def get_schema(_record_batch), do: error()

  def echo_schema(_schema), do: error()

  def get_record_batch(_schema), do: error()

  def print_record_batch(_record_batch), do: error()

  def record_batch(schema, columns) do
    columns =
      for {field, column} <- List.zip([schema.fields, columns]),
          do: prepare_column(field, column)

    %RecordBatch{reference: make_record_batch(schema, columns)}
  end

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

  #
  defp is_float_type({:f, _}), do: true
  defp is_float_type(_), do: false

  defp prepare_column(%{data_type: dtype}, column) do
    cond do
      is_float_type(dtype) -> Enum.map(column, &to_float/1)
      true -> column
    end
  end

  defp error(), do: :erlang.nif_error(:nif_not_loaded)

  defp to_float(nil), do: nil
  defp to_float(x), do: x / 1

  defp to_bool(nil), do: nil
  defp to_bool(0), do: false
  defp to_bool(1), do: true
  defp to_bool(x) when is_boolean(x), do: x

  defp to_bool(x) do
    raise "Invalid value for boolean array: #{x}"
  end

  defp to_int(nil), do: nil
  defp to_int(true), do: 1
  defp to_int(false), do: 0
  defp to_int(x) when is_integer(x), do: x

  defp to_int(x) do
    raise "Invalid integer #{x}"
  end
end
