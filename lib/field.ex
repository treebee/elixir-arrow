defmodule Arrow.Field do
  @enforce_keys [:name, :data_type, :nullable]
  defstruct [:name, :data_type, :nullable, :dict_id, :dict_is_ordered, :metadata]
end
