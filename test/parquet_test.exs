defmodule Arrow.ParquetTest do
  use ExUnit.Case
  alias Arrow.Parquet
  alias Arrow.Table

  test "read parquet file as table" do
    table = Parquet.read_table("test/data/dataframe.parquet")
    schema = Table.schema(table)
    assert length(schema.fields) == 3
    [field1, field2, field3] = schema.fields
    assert field1.data_type == {:f, 64}
    assert field2.data_type == {:f, 64}
    assert field3.data_type == {:s, 64}
  end

  test "read parquet file with restricted columns" do
    table = Parquet.read_table("test/data/dataframe.parquet", ["a", "c"])
    schema = Table.schema(table)
    assert length(schema.fields) == 2
  end
end
