defmodule Kaffe.ProducerTest do
  use ExUnit.Case

  alias Kaffe.Producer

  setup do
    Process.register(self(), :test_case)
    update_producer_config(:topics, ["topic", "topic2"])
    update_producer_config(:partition_strategy, :md5)
    TestBrod.set_produce_response(:ok)
    :ok
  end

  describe "produce_sync" do
    test "(key, value) produces a message to the first configured topic" do
      :ok = Producer.produce_sync("key8", "value")
      assert_receive [:produce_sync, "topic", 17, "key8", "value"]
    end

    test "(topic, message_list) produces messages to the specific topic" do
      :ok = Producer.produce_sync("topic2", [{"key8", "value1"}, {"key12", "value2"}])
      assert_receive [:produce_sync, "topic2", 17, "ignored", [{_ts1, "key8", "value1"}, {_ts2, "key12", "value2"}]]
    end

    test "(topic, message_list, partition_strategy) produces messages to the specific topic" do
      :ok = Producer.produce("topic2", [{"key8", "value1"}, {"key12", "value2"}], partition_strategy: :md5)
      assert_receive [:produce_sync, "topic2", 17, "ignored", [{_ts1, "key8", "value1"}, {_ts2, "key12", "value2"}]]
    end

    test "(topic, message_list, partition_strategy) produces messages to the specific topic and partition" do
      :ok =
        Producer.produce("topic2", [{"key8", "value1"}, {"key12", "value2"}],
          partition_strategy: fn _topic, _partitions_count, _key, _value -> 19 end
        )

      assert_receive [:produce_sync, "topic2", 19, "ignored", [{_ts1, "key8", "value1"}, {_ts2, "key12", "value2"}]]
    end

    test "(topic, key, value) produces a message to the specific topic" do
      :ok = Producer.produce_sync("topic2", "key8", "value")
      assert_receive [:produce_sync, "topic2", 17, "key8", "value"]
    end

    test "(topic, partition, key, value) produces a message to the specific topic/partition" do
      partition = 99
      :ok = Producer.produce_sync("topic2", partition, "key", "value")
      assert_receive [:produce_sync, "topic2", ^partition, "key", "value"]
    end

    test "(topic, partition, key, message_list) produces a list of messages to the specific topic/parition" do
      partition = 99
      :ok = Producer.produce_sync("topic2", partition, [{"key8", "value1"}, {"key12", "value2"}])

      assert_receive [
        :produce_sync,
        "topic2",
        ^partition,
        "ignored",
        [{_ts1, "key8", "value1"}, {_ts2, "key12", "value2"}]
      ]
    end

    test "passes through the result" do
      TestBrod.set_produce_response(response = {:error, {:producer_down, :noproc}})
      assert response == Producer.produce_sync("key", "value")
    end
  end

  describe "partition selection" do
    test "random" do
      update_producer_config(:partition_strategy, :random)

      :ok = Producer.produce_sync("topic2", "key", "value")
      assert_receive [:produce_sync, "topic2", random_partition, "key", "value"]

      assert 0 <= random_partition && random_partition <= 32

      :ok = Producer.produce_sync("topic2", "key", "value")
      assert_receive [:produce_sync, "topic2", random_partition, "key", "value"]

      assert 0 <= random_partition && random_partition <= 32

      :ok = Producer.produce_sync("topic2", "key", "value")
      assert_receive [:produce_sync, "topic2", random_partition, "key", "value"]

      assert 0 <= random_partition && random_partition <= 32
    end

    test "md5" do
      update_producer_config(:partition_strategy, :md5)

      :ok = Producer.produce_sync("topic2", "key1", "value")
      assert_receive [:produce_sync, "topic2", partition1, "key1", "value"]

      assert 0 <= partition1 && partition1 <= 32,
             "The partition should be in the range"

      :ok = Producer.produce_sync("topic2", "key1", "value")

      assert_receive [:produce_sync, "topic2", ^partition1, "key1", "value"],
                     "Should receive the same partition for the same key"

      :ok = Producer.produce_sync("topic2", "key2", "value")
      assert_receive [:produce_sync, "topic2", partition2, "key2", "value"]

      assert partition1 != partition2,
             "Partitions should vary"
    end

    test "given function partition strategy" do
      choose_partition = fn topic, partitions_count, key, value ->
        assert topic == "topic"
        assert partitions_count == 32
        assert key == "key"
        assert value == "value"
        0
      end

      update_producer_config(:partition_strategy, choose_partition)

      :ok = Producer.produce_sync("topic", "key", "value")
      assert_receive [:produce_sync, "topic", 0, "key", "value"]

      :ok = Producer.produce_sync("topic", "key", "value")
      assert_receive [:produce_sync, "topic", 0, "key", "value"]
    end

    test "does not use a selection strategy when given a direct partition" do
      :ok = Producer.produce_sync("topic", 0, "key", "value")
      assert_receive [:produce_sync, "topic", 0, "key", "value"]

      :ok = Producer.produce_sync("topic", 0, "key", "value")
      assert_receive [:produce_sync, "topic", 0, "key", "value"]
    end
  end

  defp update_producer_config(key, value) do
    producer_config = Application.get_env(:kaffe, :producer)
    Application.put_env(:kaffe, :producer, put_in(producer_config, [key], value))
  end
end
