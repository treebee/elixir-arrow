defmodule Arrow.Array do
  defstruct [:reference]

  alias Arrow.Conversion

  @behaviour Access

  def sum(%Arrow.Array{} = array) do
    Arrow.array_sum(array)
  end

  def min(%Arrow.Array{} = array) do
    Arrow.array_min(array)
  end

  def max(%Arrow.Array{} = array) do
    Arrow.array_max(array)
  end

  def len(%Arrow.Array{} = array) do
    Arrow.len(array)
  end

  def to_list(%Arrow.Array{} = array) do
    values = Arrow.to_list(array)

    case Arrow.Array.data_type(array) do
      {:timestamp_us, 64} -> values |> Enum.map(&Conversion.unix_to_datetime(&1, :microsecond))
      {:date, 32} -> values |> Enum.map(&Conversion.days_to_date/1)
      _ -> values
    end
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

  def debug(%Arrow.Array{} = array), do: Arrow.array_debug(array)
end

defimpl Inspect, for: Arrow.Array do
  def inspect(array, _opts) do
    Arrow.Array.debug(array)
  end
end
