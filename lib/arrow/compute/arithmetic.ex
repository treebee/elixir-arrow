defmodule Arrow.Compute.Arithmetic do
  alias Arrow.Array

  def add(%Array{} = left, %Array{} = right), do: Arrow.array_compute_add(left, right)
  def divide(%Array{} = left, %Array{} = right), do: Arrow.array_compute_divide(left, right)
  def multiply(%Array{} = left, %Array{} = right), do: Arrow.array_compute_multiply(left, right)
  def subtract(%Array{} = left, %Array{} = right), do: Arrow.array_compute_subtract(left, right)
end
