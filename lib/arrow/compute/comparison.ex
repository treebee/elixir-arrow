defmodule Arrow.Compute.Comparison do
  alias Arrow.Array

  def eq(%Array{} = left, %Array{} = right), do: Arrow.array_compute_eq(left, right)
  def neq(%Array{} = left, %Array{} = right), do: Arrow.array_compute_neq(left, right)
  def gt(%Array{} = left, %Array{} = right), do: Arrow.array_compute_gt(left, right)
  def gt_eq(%Array{} = left, %Array{} = right), do: Arrow.array_compute_gt_eq(left, right)
  def lt(%Array{} = left, %Array{} = right), do: Arrow.array_compute_lt(left, right)
  def lt_eq(%Array{} = left, %Array{} = right), do: Arrow.array_compute_lt_eq(left, right)

  def eq_scalar(%Array{} = left, right), do: Arrow.array_compute_eq_scalar(left, right)
  def neq_scalar(%Array{} = left, right), do: Arrow.array_compute_neq_scalar(left, right)
  def gt_scalar(%Array{} = left, right), do: Arrow.array_compute_gt_scalar(left, right)
  def gt_eq_scalar(%Array{} = left, right), do: Arrow.array_compute_gt_eq_scalar(left, right)
  def lt_scalar(%Array{} = left, right), do: Arrow.array_compute_lt_scalar(left, right)
  def lt_eq_scalar(%Array{} = left, right), do: Arrow.array_compute_lt_eq_scalar(left, right)
end
