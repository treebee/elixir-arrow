defmodule Arrow.Parquet do
  def write_record_batches(batches, path) do
    Arrow.Native.write_record_batches(
      Enum.map(batches, fn batch -> batch.reference end),
      Path.absname(path)
    )
  end
end
