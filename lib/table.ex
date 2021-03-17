defmodule Arrow.Table do
  alias Arrow.Field
  defstruct [:reference]

  def new(columns) do
    schema = create_schema(columns)

    Arrow.table(
      schema,
      schema.fields |> Enum.map(fn field -> Map.get(columns, String.to_atom(field.name)) end)
    )
  end

  def new(columns, schema) do
    schema = %{
      schema
      | fields: Enum.map(schema.fields, &ensure_string_names/1)
    }

    Arrow.table(schema, columns)
  end

  def schema(table), do: Arrow.get_schema(table.reference)

  def create_schema(columns) do
    fields =
      columns
      |> Enum.map(&Arrow.Field.from_column/1)

    Arrow.Schema.new(fields)
  end

  defp ensure_string_names(%Field{name: name} = field) when is_binary(name), do: field
  defp ensure_string_names(%Field{name: name} = field), do: %{field | name: Atom.to_string(name)}

  defimpl Inspect, for: Arrow.Table do
    import Inspect.Algebra

    def inspect(table, _opts) do
      schema = Arrow.Table.schema(table)
      cols = for field <- schema.fields, do: "#{field.name}:  #{type_str(field.data_type)}"
      concat(["#Arrow.Table\n", "#{Enum.join(cols, "\n")}"])
    end

    defp type_str({:s, 32}), do: "Int32"
    defp type_str({:s, 64}), do: "Int64"
    defp type_str({:u, 32}), do: "UInt32"
    defp type_str({:u, 64}), do: "UInt64"
    defp type_str({:f, 32}), do: "Float32"
    defp type_str({:f, 64}), do: "Float64"
  end
end
