defmodule Arrow.DataFusion.ExecutionContext do
  @moduledoc """
  The ExecutionContext is the main interface for executing queries with
  DataFusion. It provides functionality to
  * register a Parquet or CSV file that can be referenced from a SQL query
  * and execution of a SQL query

  ## Example

      iex> alias Arrow.DataFusion.ExecutionContext
      iex> ctx = ExecutionContext.new()
      iex> ctx = ExecutionContext.register_parquet(ctx, "example", "test/data/dataframe.parquet")
      iex> batches = ExecutionContext.sql(ctx, "SELECT a, SUM(b) AS sum_b FROM example GROUP BY a")

  """
  defstruct [:reference]

  alias Arrow.RecordBatch
  alias Arrow.Native

  def new() do
    %Arrow.DataFusion.ExecutionContext{reference: Native.create_datafusion_execution_context()}
  end

  def register_parquet(%{reference: ctx_ref} = ctx, table, path) do
    Native.datafusion_execution_context_register_parquet(ctx_ref, table, path)
    ctx
  end

  def register_csv(%{reference: ctx_ref} = ctx, table, path) do
    Native.datafusion_execution_context_register_csv(ctx_ref, table, path)
    ctx
  end

  def sql(%{reference: ctx_ref}, query) do
    Native.datafusion_execute_sql(ctx_ref, query)
    |> Enum.map(fn batch_reference -> %RecordBatch{reference: batch_reference} end)
  end
end
