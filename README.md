# ArrowElixir

Elixir bindings for [Apache Arrow](https://arrow.apache.org/).

```
NOTE:
At the moment, this is not more than a bunch of experiments,
NOT an official library of the Apache Arrow project!
```

The bindings use the [Rust implementation](https://github.com/apache/arrow/tree/master/rust) via [rustler](https://github.com/rusterlium/rustler/).
(I'm still quite new to Elixir and even less experienced with Rust, so
don't expect too much :D)

In addition to Arrow, the library comes with support for
reading and writing [Parquet](https://parquet.apache.org/) files.
