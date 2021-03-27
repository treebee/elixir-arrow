defmodule Arrow.Conversion do
  def unix_to_datetime(nil, _), do: nil
  def unix_to_datetime(ts, precision), do: DateTime.from_unix!(ts, precision)

  def days_to_date(nil), do: nil
  def days_to_date(days), do: Date.add(~D[1970-01-01], days)

  def to_float(nil), do: nil
  def to_float(x), do: x / 1

  def to_bool(nil), do: nil
  def to_bool(0), do: false
  def to_bool(1), do: true
  def to_bool(x) when is_boolean(x), do: x

  def to_bool(x) do
    raise "Invalid value for boolean array: #{x}"
  end

  def to_days(nil), do: nil
  def to_days(%Date{} = d), do: Date.diff(d, ~D[1970-01-01])

  def to_timestamp(nil, _), do: nil

  def to_timestamp(%DateTime{} = dt, :us) do
    DateTime.to_unix(dt, :microsecond)
  end

  def to_int(nil), do: nil
  def to_int(true), do: 1
  def to_int(false), do: 0
  def to_int(x) when is_integer(x), do: x

  def to_int(x) do
    raise "Invalid integer #{x}"
  end
end
