defmodule Arrow.DataFusion do
  alias Arrow.RecordBatch

  def query_parquet(path, table, query) do
    Arrow.Native.query_parquet(path, table, query)
    |> Enum.map(fn batch -> %RecordBatch{reference: batch} end)
  end
end
