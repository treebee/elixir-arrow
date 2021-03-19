defmodule Arrow.Parquet do
  def read_table(path, columns \\ []) do
    path = Path.absname(path)
    Arrow.read_table(path, columns)
  end

  def write_record_batches(path, batches) do
    Arrow.write_record_batches(
      Path.absname(path),
      Enum.map(batches, fn batch -> batch.reference end)
    )
  end
end
