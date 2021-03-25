defmodule Arrow.Array do
  defstruct [:reference]

  @behaviour Access

  def sum(%Arrow.Array{} = array) do
    Arrow.array_sum(array)
  end

  def len(%Arrow.Array{} = array) do
    Arrow.len(array)
  end

  def to_list(%Arrow.Array{} = array) do
    Arrow.to_list(array)
  end

  def slice(%Arrow.Array{} = array, offset, length) do
    Arrow.array_slice(array, offset, length)
  end

  def offset(%Arrow.Array{} = array), do: Arrow.array_offset(array)

  def is_null(%Arrow.Array{} = array, idx), do: Arrow.array_is_null(array, idx)

  def is_valid(%Arrow.Array{} = array, idx), do: Arrow.array_is_valid(array, idx)

  def null_count(%Arrow.Array{} = array), do: Arrow.array_null_count(array)

  def is_empty(%Arrow.Array{} = array), do: Arrow.array_is_empty(array)
  def data_type(%Arrow.Array{} = array), do: Arrow.array_data_type(array)

  @impl true
  def fetch(%Arrow.Array{} = array, _.._ = range),
    do: {:ok, slice(array, range.first, range.last - range.first)}

  @impl true
  def fetch(%Arrow.Array{} = array, key), do: {:ok, slice(array, key, 1)}

  @impl true
  def get_and_update(_array, _key, _function) do
    raise "Access.get_and_update/3 not implemented for Arrow.Array"
  end

  @impl true
  def pop(_array, _key) do
    raise "Access.pop/2 not implemented for Arrow.Array"
  end
end

defimpl Inspect, for: Arrow.Array do
  def inspect(array, _opts) do
    "#Arrow.#{type_str(Arrow.Array.data_type(array))}Array\n" <>
      "#{inspect(Arrow.Array.to_list(array))}"
  end

  defp type_str({:s, 8}), do: "Int8"
  defp type_str({:s, 16}), do: "Int16"
  defp type_str({:s, 32}), do: "Int32"
  defp type_str({:s, 64}), do: "Int64"
  defp type_str({:u, 1}), do: "Boolean"
  defp type_str({:u, 8}), do: "UInt8"
  defp type_str({:u, 16}), do: "UInt16"
  defp type_str({:u, 32}), do: "UInt32"
  defp type_str({:u, 64}), do: "UInt64"
  defp type_str({:f, 32}), do: "Float32"
  defp type_str({:f, 64}), do: "Float64"
  defp type_str({:utf8, 32}), do: "String"
end
