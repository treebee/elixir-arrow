defmodule Arrow.Field do
  @enforce_keys [:name, :data_type, :nullable]
  defstruct [:name, :data_type, :nullable, :metadata, dict_id: 0, dict_is_ordered: false]

  def from_column({name, values}) do
    type = Arrow.Type.infer(values)
    %Arrow.Field{name: Atom.to_string(name), data_type: type, nullable: true}
  end
end
