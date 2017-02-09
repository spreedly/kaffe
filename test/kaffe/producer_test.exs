defmodule Kaffe.ProducerTest do
  use ExUnit.Case, async: true

  alias Kaffe.Producer

  @test_partition_count Application.get_env(:kaffe, :test_partition_count)

  setup do
    %{
      client_name: :client,
      endpoints: [kafka_test: 9092],
      producer_config: Kaffe.Config.Producer.default_client_producer_config,
      partition_strategy: :round_robin,
      topics: ["topic", "topic2"],
      producer_state: %Kaffe.Producer.State{
        client: :client,
        topics: ["topic", "topic2"],
        partition_details: %{
          topic: %{partition: nil, total: 3},
          topic2: %{partition: nil, total: 20}
        },
        partition_strategy: :round_robin
      }
    }
  end

  test "on initialization Producer determines the number of partitions for each topic", c do
    assert {:ok, %{partition_details: details}} = Producer.init([c])
    c.topics
    |> Enum.each(fn(topic) ->
      assert %{partition: nil, total: @test_partition_count} == details[String.to_atom(topic)]
    end)
  end

  test "produce_sync(key, value) produces a message to the first configured topic", c do
    Producer.handle_call({:produce_sync, "key", "value"}, self, c.producer_state)
    assert_receive [:produce_sync, "topic", 0, "key", "value"]
  end

  test "produce_sync(topic, key, value) produces a message to the specific topic", c do
    Producer.handle_call({:produce_sync, "topic2", "key", "value"}, self, c.producer_state)
    assert_receive [:produce_sync, "topic2", 0, "key", "value"]
  end

  test "produce_sync(topic, partition, key, value) produces a message to the specific topic/partition", c do
    partition = 99
    Producer.handle_call(
      {:produce_sync, "topic2", partition, "key", "value"}, self, c.producer_state)
    assert_receive [:produce_sync, "topic2", ^partition, "key", "value"]
  end

  describe "partition selection" do
    test "round robin strategy", c do
      state = c.producer_state
      state = %{state | partition_strategy: :round_robin}

      {:reply, :ok, new_state} = Producer.handle_call(
        {:produce_sync, "topic", "key", "value"}, self, state)
      assert_receive [:produce_sync, "topic", 0, "key", "value"]

      {:reply, :ok, new_state} = Producer.handle_call(
        {:produce_sync, "topic", "key", "value"}, self, new_state)
      assert_receive [:produce_sync, "topic", 1, "key", "value"]

      {:reply, :ok, new_state} = Producer.handle_call(
        {:produce_sync, "topic", "key", "value"}, self, new_state)
      assert_receive [:produce_sync, "topic", 2, "key", "value"]

      {:reply, :ok, _new_state} = Producer.handle_call(
        {:produce_sync, "topic", "key", "value"}, self, new_state)
      assert_receive [:produce_sync, "topic", 0, "key", "value"]
    end

    test "random partition strategy", c do
      state = c.producer_state
      state = %{state | partition_strategy: :random}

      {:reply, :ok, new_state} = Producer.handle_call(
        {:produce_sync, "topic2", "key", "value"}, self, state)
      assert_receive [:produce_sync, "topic2", random_partition, "key", "value"]

      assert (0 <= random_partition) && (random_partition <= 19)

      {:reply, :ok, new_state} = Producer.handle_call(
        {:produce_sync, "topic2", "key", "value"}, self, new_state)
      assert_receive [:produce_sync, "topic2", random_partition, "key", "value"]

      assert (0 <= random_partition) && (random_partition <= 19)

      {:reply, :ok, _new_state} = Producer.handle_call(
        {:produce_sync, "topic2", "key", "value"}, self, new_state)
      assert_receive [:produce_sync, "topic2", random_partition, "key", "value"]

      assert (0 <= random_partition) && (random_partition <= 19)
    end

    test "md5 partition strategy", c do
      state = c.producer_state
      state = %{state | partition_strategy: :md5}

      {:reply, :ok, new_state} = Producer.handle_call(
        {:produce_sync, "topic2", "key1", "value"}, self, state)
      assert_receive [:produce_sync, "topic2", partition1, "key1", "value"]

      assert (0 <= partition1) && (partition1 <= 19),
        "The partition should be in the range"

      {:reply, :ok, new_state} = Producer.handle_call(
        {:produce_sync, "topic2", "key1", "value"}, self, new_state)
      assert_receive [:produce_sync, "topic2", ^partition1, "key1", "value"],
        "Should receive the same partition for the same key"

      {:reply, :ok, _new_state} = Producer.handle_call(
        {:produce_sync, "topic2", "key2", "value"}, self, new_state)
      assert_receive [:produce_sync, "topic2", partition2, "key2", "value"]

      assert partition1 != partition2,
        "Partitions should vary"
    end

    test "given function partition strategy", c do
      choose_partition = fn(topic, current_partition, partitions_count, key, value) ->
        assert topic == "topic"
        assert current_partition == nil || current_partition == 0
        assert partitions_count == 3
        assert key == "key"
        assert value == "value"
        0
      end

      state = c.producer_state
      state = %{state | partition_strategy: choose_partition}

      {:reply, :ok, new_state} = Producer.handle_call(
        {:produce_sync, "topic", "key", "value"}, self, state)
      assert_receive [:produce_sync, "topic", 0, "key", "value"]

      {:reply, :ok, _new_state} = Producer.handle_call(
        {:produce_sync, "topic", "key", "value"}, self, new_state)
      assert_receive [:produce_sync, "topic", 0, "key", "value"]
    end

    test "producer does not use a selection strategy when given a direct partition", c do
      {:reply, :ok, new_state} = Producer.handle_call(
       {:produce_sync, "topic", 0, "key", "value"}, self, c.producer_state)
      assert_receive [:produce_sync, "topic", 0, "key", "value"]

      Producer.handle_call(
        {:produce_sync, "topic", 0, "key", "value"}, self, new_state)
      assert_receive [:produce_sync, "topic", 0, "key", "value"]
    end
  end
end
