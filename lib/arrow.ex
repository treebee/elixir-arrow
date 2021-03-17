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

  def get_schema(_table), do: error()

  def echo_schema(_schema), do: error()

  def get_table(_schema), do: error()

  def read_table(path, columns \\ []) do
    table = read_table_parquet(path, columns)
    %Arrow.Table{reference: table}
  end

  defp read_table_parquet(_path, _columns), do: error()

  def print_table(_table), do: error()

  defp is_float_type({:f, _}), do: true
  defp is_float_type(_), do: false

  defp prepare_column(%{data_type: dtype}, column) do
    cond do
      is_float_type(dtype) -> Enum.map(column, &to_float/1)
      true -> column
    end
  end

  def table(schema, columns) do
    columns =
      for {field, column} <- List.zip([schema.fields, columns]),
          do: prepare_column(field, column)

    %Arrow.Table{reference: make_table(schema, columns)}
  end

  def make_table(_schema, _columns), do: error()

  defp error(), do: :erlang.nif_error(:nif_not_loaded)

  defp to_float(nil), do: nil
  defp to_float(x), do: x / 1
end
