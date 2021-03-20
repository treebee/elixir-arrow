# ArrowElixir

Elixir bindings for [Apache Arrow](https://arrow.apache.org/). Uses the
[Rust implementation](https://github.com/apache/arrow/tree/master/rust) via [rustler](https://github.com/rusterlium/rustler/).

**DISCLAIMER:**

- **It's NOT an offical library of the Apache Arrow project**
- It's an early WIP and mostly experimental, nothing but production ready.
- I'm quite new to Elixir and I've basically no experience with Rust, so cut me some slack, please :D.
- It's currently a 3-in-1 library, Arrow, Parquet and DataFusion, but in
  the future it would be nice to have those as separated libs

## Arrow

For Arrow there's already some basic support to create Array's and RecordBatches:

```elixir
arr = Arrow.array([1, 3, 4, nil])
#Arrow.Int64Array
[1, 3, 4, nil]
```

`RecordBatch`es are datasets of a number of contiguous arrays
with the same length including a schema:

```elixir
alias Arrow.RecordBatch

record_batch = recordbatch.new(%{cola: [3, 4, nil, 5], colb: [4.5, 3.5, nil, nil], colc: ["a", "b", "c", "d"]})
#arrow.recordbatch
cola:  int64
colb:  float32
colc:  string

RecordBatch.to_map(record_batch)
%{
  "cola" => [3, 4, nil, 5],
  "colb" => [4.5, 3.5, nil, nil],
  "colc" => ["a", "b", "c", "d"]
}

RecordBatch.schema(record_batch)
%Arrow.Schema{
  fields: [
    %Arrow.Field{
      data_type: {:s, 64},
      dict_id: 0,
      dict_is_ordered: false,
      metadata: nil,
      name: "cola",
      nullable: true
    },
    %Arrow.Field{
      data_type: {:f, 32},
      dict_id: 0,
      dict_is_ordered: false,
      metadata: nil,
      name: "colb",
      nullable: true
    },
    %Arrow.Field{
      data_type: {:utf8, 32},
      dict_id: 0,
      dict_is_ordered: false,
      metadata: nil,
      name: "colc",
      nullable: true
    }
  ],
  metadata: []
}
```

## Parquet

In addition to Arrow, the library comes with support for
reading and writing [Parquet](https://parquet.apache.org/) files.

```elixir
record_batch = RecordBatch.new(%{
    a: [1.0, 2.0, 3.0, nil, 5.0],
    b: [1.2, 5.5, 4.5, nil, nil],
    c: [0, 0, 1, 0, 1]
})
Arrow.Parquet.write_record_batches("/tmp/testdata.parquet")

batches =
  Arrow.Parquet.File.open("/tmp/testdata.parquet")
  |> Arrow.Parquet.File.iter_batches()
  |> Enum.to_list()
[#Arrow.RecordBatch
a:  Float64
b:  Float64
c:  Int64]

batches =
  |> Enum.map(&RecordBatch.to_map/1)
[
  %{
    "a" => [1.0, 2.0, 3.0, nil, 5.0],
    "b" => [1.2, 5.5, 4.5, nil, nil],
    "c" => [0, 0, 1, 0, 1]
  }
]


```

## DataFusion

Using [DataFusion](https://github.com/apache/arrow/tree/master/rust/datafusion), reading Parquet files makes even more fun:

### Querying Parquet Files With SQL

```
alias Arrow.DataFusion.ExecutionContext
batches =
  ExecutionContext.new()
  |> ExecutionContext.register_parquet("example", "/tmp/testdata.parquet")
  |> ExecutionContext.sql(ctx, "SELECT a, b, c FROM example")
[#Arrow.RecordBatch
a:  Float64
b:  Float64
c:  Int64]

batches |> hd() |> RecordBatch.to_map()
%{
    "a" => [1.0, 2.0, 3.0, nil, 5.0],
    "b" => [1.2, 5.5, 4.5, nil, nil],
    "c" => [0, 0, 1, 0, 1]
}
```

### More SQL Features - GROUP BY

```elixir
batches =
  ExecutionContext.new()
  |> ExecutionContext.register_parquet("example", "/tmp/testdata.parquet")
  |> ExecutionContext.sql(ctx, "SELECT SUM(a) as sum_a, SUM(b) as sum_b, c FROM example GROUP BY")
  |> Enum.map(&RecordBatch.to_map/1)
[%{"c" => [1, 0], "sum_a" => [8.0, 3.0], "sum_b" => [4.5, 6.7]}]
```
