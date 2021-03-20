defmodule Arrow.DataFusionTest do
  use ExUnit.Case

  alias Arrow.DataFusion.ExecutionContext

  test "read parquet file" do
    result =
      ExecutionContext.new()
      |> ExecutionContext.register_parquet("mytable", "test/data/dataframe.parquet")
      |> ExecutionContext.sql(
        "SELECT SUM(a) AS sum_a, SUM(b) as sum_b, c FROM mytable GROUP BY c ORDER BY c DESC"
      )
      |> Enum.map(&Arrow.RecordBatch.to_map/1)
      |> hd()

    assert result == %{"sum_a" => [8.0, 3.0], "sum_b" => [4.5, 6.7], "c" => [1, 0]}
  end

  test "read csv file" do
    result =
      ExecutionContext.new()
      |> ExecutionContext.register_csv("mytable", "test/data/dataframe.csv")
      |> ExecutionContext.sql(
        "SELECT SUM(a) AS sum_a, SUM(b) as sum_b, c FROM mytable GROUP BY c ORDER BY c DESC"
      )
      |> Enum.map(&Arrow.RecordBatch.to_map/1)
      |> hd()

    assert result == %{"sum_a" => [8.0, 3.0], "sum_b" => [4.5, 6.7], "c" => [1, 0]}
  end
end
