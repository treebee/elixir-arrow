defmodule Arrow.Parquet do
  def read_table(path, columns \\ []) do
    path = Path.absname(path)
    Arrow.read_table(path, columns)
  end
end
