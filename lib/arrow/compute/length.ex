defmodule Arrow.Compute.Length do
  alias Arrow.Array

  def length(%Array{} = array), do: Arrow.Native.array_compute_length(array)
end
