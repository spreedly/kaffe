defmodule Kaffe.PartitionSelectorTest do
  use ExUnit.Case, async: true
  doctest Kaffe.PartitionSelector

  alias Kaffe.PartitionSelector

  describe "random/1" do
    test "generates random values between 0 and total-1" do
      assert Enum.member?(0..0, PartitionSelector.random(1))
      assert Enum.member?(0..0, PartitionSelector.random(1))
      assert Enum.member?(0..0, PartitionSelector.random(1))

      assert Enum.member?(0..1, PartitionSelector.random(2))
      assert Enum.member?(0..1, PartitionSelector.random(2))
      assert Enum.member?(0..1, PartitionSelector.random(2))

      assert Enum.member?(0..2, PartitionSelector.random(3))
      assert Enum.member?(0..2, PartitionSelector.random(3))
      assert Enum.member?(0..2, PartitionSelector.random(3))
    end
  end
end
