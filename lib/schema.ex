defmodule Arrow.Schema do
  @enforce_keys [:fields]
  defstruct [:fields, metadata: []]

  def new(fields) do
    %Arrow.Schema{fields: fields, metadata: []}
  end
end
