defmodule Arrow.RecordBatch do
  alias Arrow.Field
  defstruct [:reference]

  def new(columns) do
    schema = create_schema(columns)

    Arrow.record_batch(
      schema,
      schema.fields |> Enum.map(fn field -> Map.get(columns, String.to_atom(field.name)) end)
    )
  end

  def new(columns, schema) do
    schema = %{
      schema
      | fields: Enum.map(schema.fields, &ensure_string_names/1)
    }

    Arrow.record_batch(schema, columns)
  end

  def schema(record_batch), do: Arrow.get_schema(record_batch.reference)

  def create_schema(columns) do
    fields =
      columns
      |> Enum.map(&Arrow.Field.from_column/1)

    Arrow.Schema.new(fields)
  end

  def to_map(record_batch), do: Arrow.record_batch_to_map(record_batch.reference)

  defp ensure_string_names(%Field{name: name} = field) when is_binary(name), do: field
  defp ensure_string_names(%Field{name: name} = field), do: %{field | name: Atom.to_string(name)}

  defimpl Inspect, for: Arrow.RecordBatch do
    import Inspect.Algebra

    def inspect(record_batch, _opts) do
      schema = Arrow.RecordBatch.schema(record_batch)
      cols = for field <- schema.fields, do: "#{field.name}:  #{type_str(field.data_type)}"
      concat(["#Arrow.RecordBatch\n", "#{Enum.join(cols, "\n")}"])
    end

    defp type_str({:s, 8}), do: "Int8"
    defp type_str({:s, 16}), do: "Int16"
    defp type_str({:s, 32}), do: "Int32"
    defp type_str({:s, 64}), do: "Int64"
    defp type_str({:u, 8}), do: "UInt8"
    defp type_str({:u, 16}), do: "UInt16"
    defp type_str({:u, 32}), do: "UInt32"
    defp type_str({:u, 64}), do: "UInt64"
    defp type_str({:f, 32}), do: "Float32"
    defp type_str({:f, 64}), do: "Float64"
    defp type_str({:utf8, 32}), do: "String"
  end
end
