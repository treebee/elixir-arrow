defmodule Arrow.Parquet.Reader do
  defstruct [:reference]

  alias Arrow.RecordBatch

  def new(path) do
    reader = Arrow.parquet_reader(path)
    %Arrow.Parquet.Reader{reference: reader}
  end

  def iter_batches(%{reference: reference}, opts \\ []) do
    Arrow.iter_batches(
      reference,
      Keyword.get(opts, :batch_size, 65_536),
      Keyword.get(opts, :columns, [])
    )
    |> Enum.map(fn batch -> %RecordBatch{reference: batch} end)
  end

  def schema_arrow(%Arrow.Parquet.Reader{reference: reference}) do
    Arrow.parquet_reader_arrow_schema(reference)
  end
end
