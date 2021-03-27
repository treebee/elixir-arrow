defmodule Arrow.Parquet.File do
  @enforce_keys [:path]
  defstruct [:path, :reader]

  alias Arrow.Parquet.Reader
  alias Arrow.Native

  def open(path) do
    reader = Reader.new(path)
    %Arrow.Parquet.File{path: path, reader: reader}
  end

  def iter_batches(%{reader: reader}, opts \\ []) do
    Reader.iter_batches(
      reader,
      batch_size: Keyword.get(opts, :batch_size, 65_536),
      columns: Keyword.get(opts, :columns, [])
    )
  end

  def schema(%{reader: reader}), do: Native.parquet_schema(reader.reference)
  def schema_arrow(%Arrow.Parquet.File{reader: reader}), do: Reader.schema_arrow(reader)
end
