defmodule Arrow.Compute.Length do
  alias Arrow.Array

  def length(%Array{} = array), do: Arrow.array_compute_length(array)
end
