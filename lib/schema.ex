defmodule Arrow.Schema do
  @enforce_keys [:fields]
  defstruct [:fields, metadata: []]
end
