defmodule Arrow.RecordBatch do
  defstruct [:reference]

  alias Arrow.Conversion
  alias Arrow.Field

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

  def schema(record_batch), do: Arrow.Native.get_schema(record_batch.reference)

  def create_schema(columns) do
    fields =
      columns
      |> Enum.map(&Arrow.Field.from_column/1)

    Arrow.Schema.new(fields)
  end

  def debug(record_batch), do: Arrow.Native.debug_record_batch(record_batch.reference)

  # TODO: correct representation of dates and datetimes
  def to_map(record_batch) do
    schema = schema(record_batch)
    rb = Arrow.Native.record_batch_to_map(record_batch.reference)

    for field <- schema.fields, into: [] do
      case field.data_type do
        {:date, 32} ->
          {field.name, Map.get(rb, field.name) |> Enum.map(&Conversion.days_to_date/1)}

        {:timestamp_us, 64} ->
          {field.name,
           Map.get(rb, field.name) |> Enum.map(&Conversion.unix_to_datetime(&1, :microsecond))}

        _ ->
          {field.name, Map.get(rb, field.name)}
      end
    end
    |> Map.new()
  end

  defp ensure_string_names(%Field{name: name} = field) when is_binary(name), do: field
  defp ensure_string_names(%Field{name: name} = field), do: %{field | name: Atom.to_string(name)}

  defimpl Inspect, for: Arrow.RecordBatch do
    def inspect(record_batch, _opts) do
      Arrow.RecordBatch.debug(record_batch)
    end
  end
end
