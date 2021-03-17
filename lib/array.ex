defmodule Arrow.Array do
  defstruct [:array, :type]

  def sum(%Arrow.Array{} = array) do
    Arrow.sum(array.array, array.type)
  end

  def len(%Arrow.Array{} = array) do
    Arrow.len(array.array, array.type)
  end

  def to_list(%Arrow.Array{} = array) do
    Arrow.to_list(array.array, array.type)
  end
end

defimpl Inspect, for: Arrow.Array do
  def inspect(%{type: type} = array, _opts) do
    "#Arrow.#{type_str(type)}Array\n" <> "#{inspect(Arrow.Array.to_list(array))}"
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
end
