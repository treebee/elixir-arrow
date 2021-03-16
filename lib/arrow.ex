defmodule Arrow do
  @moduledoc """
  `Apache Arrow` for Elixir.
  """
  use Rustler, otp_app: :arrow, crate: "arrow_nif"

  alias Arrow.Array

  @spec add(integer, integer) :: integer
  def add(_a, _b, _opt \\ :standard), do: error()

  @doc type: :creation
  def array(arg, opts \\ [])

  def array(arg, opts) do
    type = Arrow.Type.normalize!(opts[:type] || Arrow.Type.infer(arg))

    arr =
      case type do
        {:f, _} -> Enum.map(arg, &to_float/1)
        _ -> arg
      end

    %Array{array: make_array(arr, type), type: type}
  end

  def sum(_arg, _type), do: error()

  def to_list(_arg, _type), do: error()

  def len(_arg, _type), do: error()

  def make_array(_arg, _type), do: error()

  def get_field(), do: error()

  def echo_field(_field), do: error()

  def get_schema(), do: error()

  def echo_schema(_schema), do: error()

  defp error(), do: :erlang.nif_error(:nif_not_loaded)

  defp to_float(x), do: x / 1
end
