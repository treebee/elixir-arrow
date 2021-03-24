defmodule Arrow.ComputeTest do
  use ExUnit.Case
  use ExUnit.Parameterized

  alias Arrow.Array
  alias Arrow.Compute.Arithmetic

  test "add arrays" do
    a = Arrow.array([1, 2, 3])
    b = Arrow.array([4, 4, 3])
    assert Array.to_list(Arithmetic.add(a, b)) == [5, 6, 6]
  end

  test "subtract arrays" do
    a = Arrow.array([1, 2, 3])
    b = Arrow.array([4, 4, 3])
    assert Array.to_list(Arithmetic.subtract(a, b)) == [-3, -2, 0]
  end

  test "multiply arrays" do
    a = Arrow.array([1, 2, 3])
    b = Arrow.array([4, 4, 3])
    assert Array.to_list(Arithmetic.multiply(a, b)) == [4, 8, 9]
  end

  test "divide arrays" do
    a = Arrow.array([1, 2, 3])
    b = Arrow.array([4, 4, 3])
    assert Array.to_list(Arithmetic.divide(b, a)) == [4, 2, 1]
  end

  test_with_params "compare numerical arrays",
                   fn left, right, func, expected ->
                     left_arr = Arrow.array(left)
                     right_arr = Arrow.array(right)

                     assert Array.to_list(func.(left_arr, right_arr)) == expected,
                            "failed for #{inspect(func)}"
                   end do
    [
      {[1, 2, 3, nil], [2, 2, 2, 2], &Arrow.Compute.Comparison.eq/2, [false, true, false, nil]},
      {[1, 2, 3, nil], [2, 2, 2, 2], &Arrow.Compute.Comparison.neq/2, [true, false, true, nil]},
      {[1, 2, 3, nil], [2, 2, 2, 2], &Arrow.Compute.Comparison.gt_eq/2, [false, true, true, nil]},
      {[1, 2, 3, nil], [2, 2, 2, 2], &Arrow.Compute.Comparison.gt/2, [false, false, true, nil]},
      {[1, 2, 3.0, nil], [2, 2, 2.0, 2], &Arrow.Compute.Comparison.lt_eq/2,
       [true, true, false, nil]},
      {[1, 2, 3, nil], [2, 2, 2, 2], &Arrow.Compute.Comparison.lt/2, [true, false, false, nil]}
    ]
  end

  test_with_params "compare numerical array with scalar",
                   fn left, right, func, expected ->
                     left_arr = Arrow.array(left)

                     assert Array.to_list(func.(left_arr, right)) == expected,
                            "failed for #{inspect(func)}"
                   end do
    [
      {[1.0, 2, 3, nil], 1.5, &Arrow.Compute.Comparison.eq_scalar/2, [false, false, false, nil]},
      {[1, 2, 3, nil], 3, &Arrow.Compute.Comparison.neq_scalar/2, [true, true, false, nil]},
      {[1, 2, 3, nil], 3, &Arrow.Compute.Comparison.gt_eq_scalar/2, [false, false, true, nil]},
      {[1, 2, 3, nil], 2, &Arrow.Compute.Comparison.gt_scalar/2, [false, false, true, nil]},
      {[1, 2, 3.0, nil], 2.3, &Arrow.Compute.Comparison.lt_eq_scalar/2, [true, true, false, nil]},
      {[1, 2.6, 3, nil], 2.8, &Arrow.Compute.Comparison.lt_scalar/2, [true, true, false, nil]}
    ]
  end

  test_with_params "compare string arrays",
                   fn left, right, func, expected ->
                     left_arr = Arrow.array(left)
                     right_arr = Arrow.array(right)

                     assert Array.to_list(func.(left_arr, right_arr)) == expected,
                            "failed for #{inspect(func)}"
                   end do
    [
      {["a", "bb", "ccc", nil], ["a", "b", "c", "d"], &Arrow.Compute.Comparison.eq_utf8/2,
       [true, false, false, nil]},
      {["a", "bb", "ccc", nil], ["a", "b", "c", "d"], &Arrow.Compute.Comparison.neq_utf8/2,
       [false, true, true, nil]},
      {["a", "bb", "ccc", nil], ["a", "b", "c", "d"], &Arrow.Compute.Comparison.lt_utf8/2,
       [false, false, false, nil]},
      {["a", "bb", "ccc", nil], ["a", "b", "c", "d"], &Arrow.Compute.Comparison.lt_eq_utf8/2,
       [true, false, false, nil]},
      {["a", "bb", "ccc", nil], ["a", "b", "c", "d"], &Arrow.Compute.Comparison.gt_eq_utf8/2,
       [true, true, true, nil]},
      {["a", "bb", "ccc", nil], ["a", "b", "c", "d"], &Arrow.Compute.Comparison.gt_utf8/2,
       [false, true, true, nil]}
    ]
  end

  test_with_params "compare string array with scalar",
                   fn left, right, func, expected ->
                     left_arr = Arrow.array(left)

                     assert Array.to_list(func.(left_arr, right)) == expected,
                            "failed for #{inspect(func)}"
                   end do
    [
      {["a", "bb", "ccc", nil], "a", &Arrow.Compute.Comparison.eq_utf8_scalar/2,
       [true, false, false, nil]},
      {["a", "bb", "ccc", nil], "a", &Arrow.Compute.Comparison.neq_utf8_scalar/2,
       [false, true, true, nil]},
      {["a", "bb", "ccc", nil], "a", &Arrow.Compute.Comparison.lt_utf8_scalar/2,
       [false, false, false, nil]},
      {["a", "bb", "ccc", nil], "a", &Arrow.Compute.Comparison.lt_eq_utf8_scalar/2,
       [true, false, false, nil]},
      {["a", "bb", "ccc", nil], "aa", &Arrow.Compute.Comparison.gt_eq_utf8_scalar/2,
       [false, true, true, nil]},
      {["a", "bb", "ccc", nil], "a", &Arrow.Compute.Comparison.gt_utf8_scalar/2,
       [false, true, true, nil]}
    ]
  end
end
