defmodule Arrow.Array do
  defstruct [:array, :type]

  def sum(%Arrow.Array{} = array) do
    Arrow.sum(array.array)
  end

  def len(%Arrow.Array{} = array) do
    Arrow.len(array.array)
  end

  def to_list(%Arrow.Array{} = array) do
    Arrow.to_list(array.array)
  end
end

defimpl Inspect, for: Arrow.Array do
  def inspect(%{type: {:s, 64}} = array, _opts) do
    "#Arrow.Array<Int64>\n" <> "#{inspect(Arrow.Array.to_list(array))}"
  end
end
