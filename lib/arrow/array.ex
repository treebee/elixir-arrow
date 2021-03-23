defmodule Arrow.Array do
  defstruct [:reference]

  def sum(%Arrow.Array{} = array) do
    Arrow.sum(array)
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

  def is_empty(%Arrow.Array{} = array), do: Arrow.array_is_empty(array)
  def data_type(%Arrow.Array{} = array), do: Arrow.array_data_type(array)
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
  defp type_str({:u, 8}), do: "UInt8"
  defp type_str({:u, 16}), do: "UInt16"
  defp type_str({:u, 32}), do: "UInt32"
  defp type_str({:u, 64}), do: "UInt64"
  defp type_str({:f, 32}), do: "Float32"
  defp type_str({:f, 64}), do: "Float64"
  defp type_str({:utf8, 32}), do: "String"
end
