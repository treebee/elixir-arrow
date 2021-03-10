defmodule Arrow do
  @moduledoc """
  Documentation for `Arrow`.
  """
  use Rustler, otp_app: :arrow, crate: "arrow_nif"

  alias Arrow.Array

  @spec add(integer, integer) :: integer
  def add(_a, _b, _opt \\ :standard), do: error()

  @doc type: :creation
  def array(arg, opts \\ [])

  def array(arg, opts) do
    type = Arrow.Type.normalize!(opts[:type] || Arrow.Type.infer(arg))
    %Array{array: make_array(arg, type), type: type}
  end

  def make_int64_array(_arg), do: error()
  def make_uint32_array(_arg), do: error()

  def sum(_arg), do: error()

  def to_list(_arg), do: error()

  def len(_arg), do: error()

  defp make_array(arg, {:s, 64}), do: make_int64_array(arg)
  defp make_array(arg, {:u, 32}), do: make_uint32_array(arg)

  defp error(), do: :erlang.nif_error(:nif_not_loaded)
end
