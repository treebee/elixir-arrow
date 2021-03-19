defmodule Arrow.DataFusion.ExecutionContext do
  @moduledoc """
  The ExecutionContext is the main interface for executing queries with
  DataFusion. It provides functionality to
  * register a Parquet file that can be referenced from a SQL query
  * and execution of a SQL query

  Support for other data sources, including CSV files, is not yet implemented.

  ## Example

      iex> alias Arrow.DataFusion.ExecutionContext
      iex> ctx = ExecutionContext.new()
      iex> ctx = ExecutionContext.register_parquet(ctx, "example", "test/data/dataframe.parquet")
      iex> batches = ExecutionContext.sql(ctx, "SELECT a, SUM(b) AS sum_b FROM example GROUP BY a")

  """
  defstruct [:reference]

  alias Arrow.RecordBatch

  def new() do
    %Arrow.DataFusion.ExecutionContext{reference: Arrow.create_datafusion_execution_context()}
  end

  def register_parquet(%{reference: ctx_ref} = ctx, table, path) do
    Arrow.datafusion_execution_context_register_parquet(ctx_ref, table, path)
    ctx
  end

  def sql(%{reference: ctx_ref}, query) do
    Arrow.datafusion_execute_sql(ctx_ref, query)
    |> Enum.map(fn batch_reference -> %RecordBatch{reference: batch_reference} end)
  end
end
