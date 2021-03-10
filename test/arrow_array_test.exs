defmodule Arrow.ArrayTest do
  use ExUnit.Case, async: true

  alias Arrow.Array

  test "can create array without specifying type" do
    arr = Arrow.array([1, 3, 4, 5])
    assert arr.type == {:s, 64}
    assert is_reference(arr.array)
  end

  test "calculates sum of array" do
    arr = Arrow.array([4, 3, 4, 5])
    assert Array.sum(arr) == 16
  end

  test "transform arrow array to list" do
    arr = Arrow.array([4, 5, 3, 4])
    assert Array.to_list(arr) == [4, 5, 3, 4]
  end
end
