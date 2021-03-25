defmodule Arrow.Test do
  use ExUnit.Case, async: true

  alias Arrow.Array
  alias Arrow.RecordBatch
  alias Arrow.Schema
  alias Arrow.Field

  @data_types [
    {:s, 32},
    {:s, 64},
    {:u, 32},
    {:f, 32},
    {:f, 64}
  ]
  test "can create array without specifying type" do
    arr = Arrow.array([1, 3, nil, 5])
    assert is_reference(arr.reference)
  end

  test "create float array" do
    arr = Arrow.array([1.4, 5, 6.4, 3])
    assert is_reference(arr.reference)
  end

  test "create double array" do
    arr = Arrow.array([1.4, 5, 6.4, nil], type: {:f, 64})
    assert is_reference(arr.reference)
  end

  test "calculates sum of array" do
    arr = Arrow.array([4, 3, 4, 5])
    assert Array.sum(arr) == 16
    arr = Arrow.array([4.5, 6.4], type: {:f, 64})
    assert Array.sum(arr) == 10.9
  end

  test "get min of array" do
    arr = Arrow.array([4, 3, 4, 5])
    assert Array.min(arr) == 3
  end

  test "get max of array" do
    arr = Arrow.array([4.5, 6.4], type: {:f, 64})
    assert Array.max(arr) == 6.4
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

  test "create table with schema" do
    schema = %Schema{
      fields: [
        %Field{name: :col1, data_type: {:f, 32}, nullable: true},
        %Field{name: :col2, data_type: {:u, 32}, nullable: false}
      ]
    }

    table = RecordBatch.new([[1, nil, 4], [3, 5, 3]], schema)
    assert is_reference(table.reference)
    schema = RecordBatch.schema(table)
    [field, _] = schema.fields
    assert field.name == "col1"
  end

  test "infer schema for new table" do
    table = RecordBatch.new(%{col1: [1, 3, nil, 4], col2: [5.43, 4.5, nil, nil]})
    assert is_reference(table.reference)
    schema = RecordBatch.schema(table)
    [field1, field2] = schema.fields
    assert field1.name == "col1"
    assert field2.name == "col2"
  end

  test "create slice of array" do
    arr = Arrow.array([1, 54, 3, 4])
    slice = Array.slice(arr, 1, 2)
    assert Array.to_list(slice) == [54, 3]
  end

  test "array is_empty" do
    arr = Arrow.array([1, 3, 4])
    assert Array.is_empty(arr) == false
    arr = Arrow.array([])
    assert Array.is_empty(arr) == true
  end

  test "Array Access behaviour" do
    arr = Arrow.array([4, 6, 3, 5])
    assert Arrow.Array.to_list(arr[1..3]) == [6, 3]
    assert Arrow.Array.to_list(arr[3]) == [5]
  end

  test "get Array offset" do
    arr = Arrow.array([4, 5, 3, 2])
    assert Array.offset(arr) == 0
    assert Array.offset(arr[2..4]) == 2
  end

  test "array element is_null" do
    arr = Arrow.array([1, nil, 3])
    assert Array.is_null(arr, 0) == false
    assert Array.is_null(arr, 1) == true
  end

  test "array element is_valid" do
    arr = Arrow.array([1, nil, 3])
    assert Array.is_valid(arr, 0) == true
    assert Array.is_valid(arr, 1) == false
  end

  test "array null_count" do
    arr = Arrow.array([nil, 1, nil, 4, nil])
    assert Array.null_count(arr) == 3
  end

  test "create boolean array" do
    arr = Arrow.array([true, false, true])
    assert Array.data_type(arr) == {:u, 1}
    arr = Arrow.array([1, 0, 1, 1, nil], type: {:u, 1})
    assert Array.to_list(arr) == [true, false, true, true, nil]
  end
end
