defmodule Arrow.Parquet.FileTest do
  use ExUnit.Case

  alias Arrow.Parquet.File
  alias Arrow.RecordBatch

  @testfile "test/data/dataframe.parquet"
  test "can iterate over batches" do
    file = File.open(@testfile)
    batches = Arrow.Parquet.File.iter_batches(file) |> Enum.to_list()
    assert length(batches) == 1
  end

  test "can iterate over batches with given batch_size" do
    file = File.open(@testfile)
    batches = Arrow.Parquet.File.iter_batches(file, batch_size: 1) |> Enum.to_list()
    assert length(batches) == 5
  end

  test "can restrict columns when iterating over batches" do
    file = File.open(@testfile)

    batch =
      Arrow.Parquet.File.iter_batches(file, columns: ["a", "c"]) |> Enum.to_list() |> List.first()

    schema = RecordBatch.schema(batch)
    assert Enum.map(schema.fields, fn f -> f.name end) == ["a", "c"]
  end

  test "transform RecordBatch to elixir map" do
    file = File.open(@testfile)

    batch =
      Arrow.Parquet.File.iter_batches(file) |> Enum.to_list() |> List.first()

    assert RecordBatch.to_map(batch) ==  %{
      "a" => [1.0, 2.0, 3.0, nil, 5.0],
      "b" => [1.2, 5.5, 4.5, nil, nil],
      "c" => [0, 0, 1, 0, 1]
    }
  end

end
