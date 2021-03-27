defmodule Arrow.Compute.Arithmetic do
  alias Arrow.Array
  alias Arrow.Native

  def add(%Array{} = left, %Array{} = right), do: Native.array_compute_add(left, right)
  def divide(%Array{} = left, %Array{} = right), do: Native.array_compute_divide(left, right)
  def multiply(%Array{} = left, %Array{} = right), do: Native.array_compute_multiply(left, right)
  def subtract(%Array{} = left, %Array{} = right), do: Native.array_compute_subtract(left, right)
end
