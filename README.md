# ArrowElixir

Elixir bindings for [Apache Arrow](https://arrow.apache.org/). Uses the
[Rust implementation](https://github.com/apache/arrow/tree/master/rust) via [rustler](https://github.com/rusterlium/rustler/).

**DISCLAIMER:**

- **It's NOT an offical library of the Apache Arrow project**
- It's an early WIP and mostly experimental, nothing but production ready.
- I'm quite new to Elixir and I've basically no experience with Rust, so cut me some slack, please :D.
- It's currently a 3-in-1 library, Arrow, Parquet and DataFusion, but in
  the future it would be nice to have those as separated libs

## Installation

Make sure to have [Rust](https://rustup.rs/) installed.

```elixir
defp deps do
  [
    {:arrow, git: "https://github.com/treebee/elixir-arrow.git" }
  ]
end
```

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

record_batch = RecordBatch.new(%{cola: [3, 4, nil, 5], colb: [4.5, 3.5, nil, nil], colc: ["a", "b", "c", "d"]})
#Arrow.Recordbatch
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

Using [DataFusion](https://github.com/apache/arrow/tree/master/rust/datafusion), reading Parquet files (and also CSV) makes even more fun:

### Querying Parquet Files With SQL

```elixir
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

### Mini Data Pipeline

Let's load a CSV with a GROUP BY and save back the result as Parquet:

```elixir
ExecutionContext.new()
  |> ExecutionContext.register_csv("example", "/tmp/testdata.csv")
  |> ExecutionContext.sql(ctx, "SELECT SUM(a) as sum_a, SUM(b) as sum_b, c FROM example GROUP BY")
  |> Arrow.Parquet.write_record_batches("/tmp/testdata-result.parquet")
```

## Next Steps?

What be nice to find some people interested in contributing. Really helpful
would be people with Rust experience, but everyone is welcome of course :)

Even given that the current Rust nif is not too bad, there are much more
things left to do than what is already implemented.
For example

- support for more datatypes, for example dates and datetimes
- more array operations
- reading and writing Parquet files with different options (compression, row groups, ...)
- reading/writing multiple files, partitioning, ...
- a ["Table" representation](https://arrow.apache.org/docs/python/data.html#tables) ?
- ...

Another thing, as already mentioned: Splitting Arrow, Parquet and DataFusion
into 3 different libs. (I made a short attempt to do this but ran into linker issues)

I also haven't thought too much about providing a nice API, the current
goal was rather to make some first examples work (also, no error handling yet).
But it will probably make sense to think more about how the lib integrates
nicely with the Elixir ecosystem.

Speaking about the Elixir ecosystem: How about a [Nx](https://github.com/elixir-nx/nx) Arrow backend :D

For DataFusion maybe some kind of Ecto adapter?
