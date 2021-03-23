defmodule Arrow.ComputeTest do
  use ExUnit.Case

  alias Arrow.Array
  alias Arrow.Compute.Arithmetic

  test "add arrays" do
    a = Arrow.array([1, 2, 3])
    b = Arrow.array([4, 4, 3])
    assert Array.to_list(Arithmetic.add(a, b)) == [5, 6, 6]
  end

  test "subtract arrays" do
    a = Arrow.array([1, 2, 3])
    b = Arrow.array([4, 4, 3])
    assert Array.to_list(Arithmetic.subtract(a, b)) == [-3, -2, 0]
  end

  test "multiply arrays" do
    a = Arrow.array([1, 2, 3])
    b = Arrow.array([4, 4, 3])
    assert Array.to_list(Arithmetic.multiply(a, b)) == [4, 8, 9]
  end

  test "divide arrays" do
    a = Arrow.array([1, 2, 3])
    b = Arrow.array([4, 4, 3])
    assert Array.to_list(Arithmetic.divide(b, a)) == [4, 2, 1]
  end
end
