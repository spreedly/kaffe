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

  describe "md5/2" do
    test "computes the remainder of an md5 hash of the key" do

      assert PartitionSelector.md5("key1", 10) == PartitionSelector.md5("key1", 10),
        "Partition should be deterministic"

      assert PartitionSelector.md5("key1", 10) != PartitionSelector.md5("key2", 10),
        "Partition should not be fixed"
    end
  end
end
