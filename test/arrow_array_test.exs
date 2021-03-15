defmodule Arrow.ArrayTest do
  use ExUnit.Case, async: true

  alias Arrow.Array

  @data_types [
    {:s, 32},
    {:s, 64},
    {:u, 32},
    {:f, 32},
    {:f, 64}
  ]
  test "can create array without specifying type" do
    arr = Arrow.array([1, 3, 4, 5])
    assert arr.type == {:s, 64}
    assert is_reference(arr.array)
  end

  test "create float array" do
    arr = Arrow.array([1.4, 5, 6.4, 3])
    assert arr.type == {:f, 32}
    assert is_reference(arr.array)
  end

  test "create double array" do
    arr = Arrow.array([1.4, 5, 6.4, 3], type: {:f, 64})
    assert arr.type == {:f, 64}
    assert is_reference(arr.array)
  end

  test "calculates sum of array" do
    arr = Arrow.array([4, 3, 4, 5])
    assert Array.sum(arr) == 16
    arr = Arrow.array([4.5, 6.4], type: {:f, 64})
    assert Array.sum(arr) == 10.9
  end

  test "transform arrow array to list" do
    arr = Arrow.array([4, 5, 3, 4])
    assert Array.to_list(arr) == [4, 5, 3, 4]
    arr = Arrow.array([4.5, 6.55], type: {:f, 64})
    assert Array.to_list(arr) == [4.5, 6.55]
  end

  test "array length" do
    for type <- @data_types do
      arr = Arrow.array([4, 56, 4, 5, 6, 3, 5], type: type)
      assert Array.len(arr) == 7
    end
  end
end
