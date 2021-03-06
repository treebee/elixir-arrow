defmodule Arrow.Compute.Comparison do
  alias Arrow.Array
  alias Arrow.Native

  def eq(%Array{} = left, %Array{} = right), do: Native.array_compute_eq(left, right)
  def neq(%Array{} = left, %Array{} = right), do: Native.array_compute_neq(left, right)
  def gt(%Array{} = left, %Array{} = right), do: Native.array_compute_gt(left, right)
  def gt_eq(%Array{} = left, %Array{} = right), do: Native.array_compute_gt_eq(left, right)
  def lt(%Array{} = left, %Array{} = right), do: Native.array_compute_lt(left, right)
  def lt_eq(%Array{} = left, %Array{} = right), do: Native.array_compute_lt_eq(left, right)

  def eq_utf8(%Array{} = left, %Array{} = right), do: Native.array_compute_eq_utf8(left, right)
  def neq_utf8(%Array{} = left, %Array{} = right), do: Native.array_compute_neq_utf8(left, right)
  def gt_utf8(%Array{} = left, %Array{} = right), do: Native.array_compute_gt_utf8(left, right)

  def gt_eq_utf8(%Array{} = left, %Array{} = right),
    do: Native.array_compute_gt_eq_utf8(left, right)

  def lt_utf8(%Array{} = left, %Array{} = right), do: Native.array_compute_lt_utf8(left, right)

  def lt_eq_utf8(%Array{} = left, %Array{} = right),
    do: Native.array_compute_lt_eq_utf8(left, right)

  def like_utf8(%Array{} = left, %Array{} = right),
    do: Native.array_compute_like_utf8(left, right)

  def nlike_utf8(%Array{} = left, %Array{} = right),
    do: Native.array_compute_nlike_utf8(left, right)

  def eq_scalar(%Array{} = left, right), do: Native.array_compute_eq_scalar(left, right)
  def neq_scalar(%Array{} = left, right), do: Native.array_compute_neq_scalar(left, right)
  def gt_scalar(%Array{} = left, right), do: Native.array_compute_gt_scalar(left, right)
  def gt_eq_scalar(%Array{} = left, right), do: Native.array_compute_gt_eq_scalar(left, right)
  def lt_scalar(%Array{} = left, right), do: Native.array_compute_lt_scalar(left, right)
  def lt_eq_scalar(%Array{} = left, right), do: Native.array_compute_lt_eq_scalar(left, right)

  def eq_utf8_scalar(%Array{} = left, right),
    do: Native.array_compute_eq_utf8_scalar(left, right)

  def neq_utf8_scalar(%Array{} = left, right),
    do: Native.array_compute_neq_utf8_scalar(left, right)

  def gt_utf8_scalar(%Array{} = left, right),
    do: Native.array_compute_gt_utf8_scalar(left, right)

  def gt_eq_utf8_scalar(%Array{} = left, right),
    do: Native.array_compute_gt_eq_utf8_scalar(left, right)

  def lt_utf8_scalar(%Array{} = left, right),
    do: Native.array_compute_lt_utf8_scalar(left, right)

  def lt_eq_utf8_scalar(%Array{} = left, right),
    do: Native.array_compute_lt_eq_utf8_scalar(left, right)

  def like_utf8_scalar(%Array{} = left, right),
    do: Native.array_compute_like_utf8_scalar(left, right)

  def nlike_utf8_scalar(%Array{} = left, right),
    do: Native.array_compute_nlike_utf8_scalar(left, right)
end
