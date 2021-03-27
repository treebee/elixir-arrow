defmodule Arrow.Array do
  defstruct [:reference]

  alias Arrow.Conversion
  alias Arrow.Native

  @behaviour Access

  @type t :: %Arrow.Array{}

  @spec sum(t()) :: number() | nil
  def sum(%Arrow.Array{} = array) do
    Native.array_sum(array)
  end

  @spec min(t()) :: number() | nil
  def min(%Arrow.Array{} = array) do
    Native.array_min(array)
  end

  @spec max(t()) :: number() | nil
  def max(%Arrow.Array{} = array) do
    Native.array_max(array)
  end

  @spec len(t()) :: integer()
  def len(%Arrow.Array{} = array) do
    Native.len(array)
  end

  @spec len(t()) :: list(term())
  def to_list(%Arrow.Array{} = array) do
    values = Native.to_list(array)

    case data_type(array) do
      {:timestamp_us, 64} -> values |> Enum.map(&Conversion.unix_to_datetime(&1, :microsecond))
      {:date, 32} -> values |> Enum.map(&Conversion.days_to_date/1)
      _ -> values
    end
  end

  @spec slice(t(), integer(), integer()) :: t()
  def slice(%Arrow.Array{} = array, offset, length) do
    Native.array_slice(array, offset, length)
  end

  @spec offset(t()) :: integer()
  def offset(%Arrow.Array{} = array), do: Native.array_offset(array)

  @spec is_null(t(), integer()) :: boolean()
  def is_null(%Arrow.Array{} = array, idx), do: Native.array_is_null(array, idx)

  @spec is_valid(t(), integer()) :: boolean()
  def is_valid(%Arrow.Array{} = array, idx), do: Native.array_is_valid(array, idx)

  @spec null_count(t()) :: integer()
  def null_count(%Arrow.Array{} = array), do: Native.array_null_count(array)

  @spec is_empty(t()) :: boolean()
  def is_empty(%Arrow.Array{} = array), do: Native.array_is_empty(array)

  @spec data_type(t()) :: Arrow.Type.t()
  def data_type(%Arrow.Array{} = array), do: Native.array_data_type(array)

  @impl true
  @spec fetch(t(), Range.t()) :: {:ok, t()}
  def fetch(%Arrow.Array{} = array, _.._ = range),
    do: {:ok, slice(array, range.first, range.last - range.first)}

  @impl true
  @spec fetch(t(), integer()) :: {:ok, t()}
  def fetch(%Arrow.Array{} = array, key), do: {:ok, slice(array, key, 1)}

  @impl true
  def get_and_update(_array, _key, _function) do
    raise "Access.get_and_update/3 not implemented for Arrow.Array"
  end

  @impl true
  def pop(_array, _key) do
    raise "Access.pop/2 not implemented for Arrow.Array"
  end

  @spec debug(t()) :: String.t()
  def debug(%Arrow.Array{} = array), do: Native.array_debug(array)
end

defimpl Inspect, for: Arrow.Array do
  def inspect(array, _opts) do
    Arrow.Array.debug(array)
  end
end
