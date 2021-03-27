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

  alias Arrow.Conversion

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
        {:f, _} -> Enum.map(arg, &Conversion.to_float/1)
        {:u, 1} -> Enum.map(arg, &Conversion.to_bool/1)
        {:u, _} -> Enum.map(arg, &Conversion.to_int/1)
        {:s, _} -> Enum.map(arg, &Conversion.to_int/1)
        {:timestamp_us, _} -> Enum.map(arg, &Conversion.to_timestamp(&1, :us))
        {:date, 32} -> Enum.map(arg, &Conversion.to_days/1)
        _ -> arg
      end

    Arrow.Native.make_array(arr, type)
  end

  def record_batch(schema, columns) do
    columns =
      for {field, column} <- List.zip([schema.fields, columns]),
          do: prepare_column(field, column)

    %Arrow.RecordBatch{reference: Arrow.Native.make_record_batch(schema, columns)}
  end

  defp prepare_column(%{data_type: dtype}, column) do
    case dtype do
      {:f, _} -> Enum.map(column, &Conversion.to_float/1)
      {:timestamp_us, 64} -> Enum.map(column, &Conversion.to_timestamp(&1, :us))
      {:date, 32} -> Enum.map(column, &Conversion.to_days/1)
      _ -> column
    end
  end
end
