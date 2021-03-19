defmodule Arrow.Parquet do

  def write_record_batches(path, batches) do
    Arrow.write_record_batches(
      Path.absname(path),
      Enum.map(batches, fn batch -> batch.reference end)
    )
  end
end
