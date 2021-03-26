# taken from https://github.com/elixir-nx/nx/blob/main/nx/lib/nx/type.ex
defmodule Arrow.Type do
  @moduledoc """
  Conveniences for working with types.
  A type is a two-element tuple with the name and the size.
  The first element must be one of followed by the respective
  sizes:
      * `:s` - signed integer (8, 16, 32, 64)
      * `:u` - unsigned integer (8, 16, 32, 64)
      * `:f` - float (32, 64)
      * `:utf8` - string  # TODO
  """

  @type t ::
          {:s, 8}
          | {:s, 16}
          | {:s, 32}
          | {:s, 64}
          # {:u, 1} -> boolean
          | {:u, 1}
          | {:u, 8}
          | {:u, 16}
          | {:u, 32}
          | {:u, 64}
          | {:f, 32}
          | {:f, 64}
          # {:utf8, 32} is for a GenericStringArray<i32> on Rust side
          | {:utf8, 32}
          # Timestamp(Microsecond, None)
          | {:timestamp_us, 64}

  @doc """
  Returns the minimum possible value for the given type.
  """
  def min_value_binary(type)

  def min_value_binary({:s, 8}), do: <<-128::8-signed-native>>
  def min_value_binary({:s, 16}), do: <<-32678::16-signed-native>>
  def min_value_binary({:s, 32}), do: <<-2_147_483_648::32-signed-native>>
  def min_value_binary({:s, 64}), do: <<-9_223_372_036_854_775_808::64-signed-native>>
  def min_value_binary({:u, size}), do: <<0::size(size)-native>>
  def min_value_binary({:bf, 16}), do: <<0xFF80::16-native>>
  def min_value_binary({:f, 32}), do: <<0xFF7FFFFF::32-native>>
  def min_value_binary({:f, 64}), do: <<0xFFEFFFFFFFFFFFFF::64-native>>

  @doc """
  Returns the minimum possible value for the given type.
  """
  def max_value_binary(type)

  def max_value_binary({:s, 8}), do: <<127::8-signed-native>>
  def max_value_binary({:s, 16}), do: <<32677::16-signed-native>>
  def max_value_binary({:s, 32}), do: <<2_147_483_647::32-signed-native>>
  def max_value_binary({:s, 64}), do: <<9_223_372_036_854_775_807::64-signed-native>>
  def max_value_binary({:u, 8}), do: <<255::8-native>>
  def max_value_binary({:u, 16}), do: <<65535::16-native>>
  def max_value_binary({:u, 32}), do: <<4_294_967_295::32-native>>
  def max_value_binary({:u, 64}), do: <<18_446_744_073_709_551_615::64-native>>
  def max_value_binary({:bf, 16}), do: <<0x7F80::16-native>>
  def max_value_binary({:f, 32}), do: <<0x7F7FFFFF::32-native>>
  def max_value_binary({:f, 64}), do: <<0x7FEFFFFFFFFFFFFF::64-native>>

  @doc """
  Infers the type of the given value.
  The value may be a number, boolean, or an arbitrary list with
  any of the above. Integers are by default signed and of size 64.
  Floats have size of 64. Booleans are unsigned integers of size 1
  (also known as predicates).
  In case mixed types are given, the one with highest space
  requirements is used (i.e. float > brain floating > integer > boolean).
  ## Examples
      iex> Nx.Type.infer([1, 2, 3])
      {:s, 64}
      iex> Nx.Type.infer([[1, 2], [3, 4]])
      {:s, 64}
      iex> Nx.Type.infer([1.0, 2.0, 3.0])
      {:f, 32}
      iex> Nx.Type.infer([1, 2.0])
      {:f, 32}
      iex> Nx.Type.infer([])
      {:f, 32}
      iex> Nx.Type.infer("string")
      ** (ArgumentError) cannot infer the numerical type of "string"
  """
  def infer(value) do
    case infer(value, -1) do
      -1 -> {:f, 32}
      0 -> {:u, 1}
      1 -> {:s, 64}
      2 -> {:f, 32}
      {:utf8, 32} -> {:utf8, 32}
      {:timestamp_us, 64} -> {:timestamp_us, 64}
    end
  end

  defp infer(arg, _inferred) when is_binary(arg), do: {:utf8, 32}
  defp infer(%DateTime{} = _arg, _inferred), do: {:timestamp_us, 64}
  defp infer(arg, inferred) when is_list(arg), do: Enum.reduce(arg, inferred, &infer/2)
  defp infer(arg, inferred) when is_boolean(arg), do: max(inferred, 0)
  defp infer(arg, inferred) when is_integer(arg), do: max(inferred, 1)
  defp infer(arg, inferred) when is_float(arg), do: max(inferred, 2)
  defp infer(nil, inferred), do: max(inferred, 0)

  defp infer(other, _inferred),
    do: raise(ArgumentError, "cannot infer the numerical type of #{inspect(other)}")

  @doc """
  Validates the given type tuple.
  It returns the type itself or raises.
  ## Examples
      iex> Arrow.Type.normalize!({:u, 8})
      {:u, 8}
      iex> Arrow.Type.normalize!({:u, 0})
      ** (ArgumentError) invalid numerical type: {:u, 0} (see Arrow.Type docs for all supported types)
      iex> Arrow.Type.normalize!({:k, 8})
      ** (ArgumentError) invalid numerical type: {:k, 8} (see Arrow.Type docs for all supported types)
  """
  def normalize!(type) do
    case validate(type) do
      :error ->
        raise ArgumentError,
              "invalid numerical type: #{inspect(type)} (see Arrow.Type docs for all supported types)"

      type ->
        type
    end
  end

  defp validate({:s, size} = type) when size in [8, 16, 32, 64], do: type
  defp validate({:u, size} = type) when size in [1, 8, 16, 32, 64], do: type
  defp validate({:f, size} = type) when size in [32, 64], do: type
  defp validate({:utf8, size} = type) when size in [32, 64], do: type
  defp validate({:timestamp_us, size} = type) when size in [64], do: type
  defp validate({:bf, size} = type) when size in [16], do: type
  defp validate(_type), do: :error
end
