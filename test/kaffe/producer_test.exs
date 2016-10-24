defmodule Kaffe.ProducerTest do
  use ExUnit.Case, async: true

  alias Kaffe.Producer

  @test_partition_count Application.get_env(:kaffe, :test_partition_count)

  setup do
    %{
      client: :client,
      strategy: :round_robin,
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
    assert {:ok, %{partition_details: details}} = Producer.init([c.client, c.topics, c.strategy])
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
        {:produce_sync, "topic", "key", "value"}, self, state)
      assert_receive [:produce_sync, "topic", random_partition, "key", "value"]

      assert (0 <= random_partition) && (random_partition <= 19)

      {:reply, :ok, new_state} = Producer.handle_call(
        {:produce_sync, "topic", "key", "value"}, self, new_state)
      assert_receive [:produce_sync, "topic", random_partition, "key", "value"]

      assert (0 <= random_partition) && (random_partition <= 19)

      {:reply, :ok, _new_state} = Producer.handle_call(
        {:produce_sync, "topic", "key", "value"}, self, new_state)
      assert_receive [:produce_sync, "topic", random_partition, "key", "value"]

      assert (0 <= random_partition) && (random_partition <= 19)
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
