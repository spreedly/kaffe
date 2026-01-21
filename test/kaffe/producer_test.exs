defmodule Kaffe.ProducerTest do
  use ExUnit.Case

  alias Kaffe.Producer

  @default_client_config_key :producer_name
  @default_client_name :kaffe_producer_client_producer_name

  setup do
    Process.register(self(), :test_case)
    update_producer_config(@default_client_config_key, :topics, ["topic", "topic2"])
    update_producer_config(@default_client_config_key, :partition_strategy, :md5)
    TestBrod.set_produce_response(:ok)
    :ok
  end

  describe "produce_sync" do
    test "(key, value) produces a message to the first configured topic" do
      :ok = Producer.produce_sync("key8", "value")
      assert_receive [:produce_sync, @default_client_name, "topic", 17, "key8", "value"]
    end

    test "(topic, message_list) produces messages to the specific topic" do
      :ok = Producer.produce_sync("topic2", [{"key8", "value1"}, {"key12", "value2"}])

      assert_receive [
        :produce_sync,
        @default_client_name,
        "topic2",
        17,
        "ignored",
        [{_ts1, "key8", "value1"}, {_ts2, "key12", "value2"}]
      ]
    end

    test "(topic, message_list, partition_strategy) produces messages to the specific topic" do
      :ok = Producer.produce("topic2", [{"key8", "value1"}, {"key12", "value2"}], partition_strategy: :md5)

      assert_receive [
        :produce_sync,
        @default_client_name,
        "topic2",
        17,
        "ignored",
        [{_ts1, "key8", "value1"}, {_ts2, "key12", "value2"}]
      ]
    end

    test "(topic, message_list, partition_strategy) produces messages to the specific topic and partition" do
      :ok =
        Producer.produce("topic2", [{"key8", "value1"}, {"key12", "value2"}],
          partition_strategy: fn _topic, _partitions_count, _key, _value -> 19 end
        )

      assert_receive [
        :produce_sync,
        @default_client_name,
        "topic2",
        19,
        "ignored",
        [{_ts1, "key8", "value1"}, {_ts2, "key12", "value2"}]
      ]
    end

    test "(topic, key, value) produces a message to the specific topic" do
      :ok = Producer.produce_sync("topic2", "key8", "value")
      assert_receive [:produce_sync, @default_client_name, "topic2", 17, "key8", "value"]
    end

    test "(topic, partition, key, value) produces a message to the specific topic/partition" do
      partition = 99
      :ok = Producer.produce_sync("topic2", partition, "key", "value")
      assert_receive [:produce_sync, @default_client_name, "topic2", ^partition, "key", "value"]
    end

    test "(topic, partition, key, message_list) produces a list of messages to the specific topic/partition" do
      partition = 99
      :ok = Producer.produce_sync("topic2", partition, [{"key8", "value1"}, {"key12", "value2"}])

      assert_receive [
        :produce_sync,
        @default_client_name,
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

  describe "produce_sync_with_client" do
    setup do
      current_producers_config = Application.get_env(:kaffe, :producers)

      producers_config = [
        producer_1: [
          endpoints: [kafka1: 9092],
          topics: ["topic", "topic2"],
          partition_strategy: :md5
        ],
        producer_2: [
          endpoints: [kafka2: 9092],
          topics: ["topic"],
          partition_strategy: :md5
        ]
      ]

      Application.put_env(:kaffe, :producers, producers_config)
      on_exit(fn -> Application.put_env(:kaffe, :producers, current_producers_config) end)

      producers =
        producers_config
        |> Keyword.keys()
        |> Enum.map(&{&1, Kaffe.Config.Producer.configuration(&1)})

      %{producers: producers}
    end

    test "(key, value) produces a message to the first configured topic with specified client", %{producers: producers} do
      Enum.each(producers, fn {config_key, %{client_name: client_name, topics: [topic | _]}} ->
        :ok = Producer.produce_sync_with_client(config_key, "key8", "value")
        assert_receive [:produce_sync, ^client_name, ^topic, 17, "key8", "value"]
      end)
    end

    test "(topic, message_list) produces messages to the specific topic with specified client", %{producers: producers} do
      Enum.each(producers, fn {config_key, %{client_name: client_name}} ->
        :ok = Producer.produce_sync_with_client(config_key, "topic2", [{"key8", "value1"}, {"key12", "value2"}])

        assert_receive [
          :produce_sync,
          ^client_name,
          "topic2",
          17,
          "ignored",
          [{_ts1, "key8", "value1"}, {_ts2, "key12", "value2"}]
        ]
      end)
    end

    test "(topic, message_list, partition_strategy) produces messages to the specific topic with specified client",
         %{producers: producers} do
      Enum.each(producers, fn {config_key, %{client_name: client_name}} ->
        :ok =
          Producer.produce_with_client(config_key, "topic2", [{"key8", "value1"}, {"key12", "value2"}],
            partition_strategy: :md5
          )

        assert_receive [
          :produce_sync,
          ^client_name,
          "topic2",
          17,
          "ignored",
          [{_ts1, "key8", "value1"}, {_ts2, "key12", "value2"}]
        ]
      end)
    end

    test "(topic, message_list, partition_strategy) produces messages to the specific topic and partition with specified client",
         %{producers: producers} do
      Enum.each(producers, fn {config_key, %{client_name: client_name}} ->
        :ok =
          Producer.produce_with_client(config_key, "topic2", [{"key8", "value1"}, {"key12", "value2"}],
            partition_strategy: fn _topic, _partitions_count, _key, _value -> 19 end
          )

        assert_receive [
          :produce_sync,
          ^client_name,
          "topic2",
          19,
          "ignored",
          [{_ts1, "key8", "value1"}, {_ts2, "key12", "value2"}]
        ]
      end)
    end

    test "(topic, key, value) produces a message to the specific topic with specified client", %{producers: producers} do
      Enum.each(producers, fn {config_key, %{client_name: client_name}} ->
        :ok = Producer.produce_sync_with_client(config_key, "topic2", "key8", "value")
        assert_receive [:produce_sync, ^client_name, "topic2", 17, "key8", "value"]
      end)
    end

    test "(topic, partition, key, value) produces a message to the specific topic/partition with specified client",
         %{producers: producers} do
      Enum.each(producers, fn {config_key, %{client_name: client_name}} ->
        partition = 99
        :ok = Producer.produce_sync_with_client(config_key, "topic2", partition, "key", "value")
        assert_receive [:produce_sync, ^client_name, "topic2", ^partition, "key", "value"]
      end)
    end

    test "(topic, partition, key, message_list) produces a list of messages to the specific topic/partition with specified client",
         %{producers: producers} do
      Enum.each(producers, fn {config_key, %{client_name: client_name}} ->
        partition = 99

        :ok =
          Producer.produce_sync_with_client(config_key, "topic2", partition, [{"key8", "value1"}, {"key12", "value2"}])

        assert_receive [
          :produce_sync,
          ^client_name,
          "topic2",
          ^partition,
          "ignored",
          [{_ts1, "key8", "value1"}, {_ts2, "key12", "value2"}]
        ]
      end)
    end

    test "passes through the result", %{producers: producers} do
      Enum.each(producers, fn {config_key, _} ->
        TestBrod.set_produce_response(response = {:error, {:producer_down, :noproc}})
        assert response == Producer.produce_sync_with_client(config_key, "key", "value")
      end)
    end

    test "raises an error if a non-existent config key is passed" do
      invalid_config_key = :unknown_producer
      config = Application.get_env(:kaffe, :producers)

      assert_raise KeyError, "key #{inspect(invalid_config_key)} not found in: #{inspect(config, pretty: true)}", fn ->
        :ok = Producer.produce_sync_with_client(invalid_config_key, "key8", "value")
      end
    end
  end

  describe "partition selection" do
    test "random" do
      update_producer_config(@default_client_config_key, :partition_strategy, :random)

      :ok = Producer.produce_sync("topic2", "key", "value")
      assert_receive [:produce_sync, @default_client_name, "topic2", random_partition, "key", "value"]

      assert 0 <= random_partition && random_partition <= 32

      :ok = Producer.produce_sync("topic2", "key", "value")
      assert_receive [:produce_sync, @default_client_name, "topic2", random_partition, "key", "value"]

      assert 0 <= random_partition && random_partition <= 32

      :ok = Producer.produce_sync("topic2", "key", "value")
      assert_receive [:produce_sync, @default_client_name, "topic2", random_partition, "key", "value"]

      assert 0 <= random_partition && random_partition <= 32
    end

    test "md5" do
      System.put_env("KAFFE_PRODUCER_USER", "Alice")
      System.put_env("KAFFE_PRODUCER_PASSWORD", "ecilA")
      update_producer_config(@default_client_config_key, :partition_strategy, :md5)

      :ok = Producer.produce_sync("topic2", "key1", "value")
      assert_receive [:produce_sync, @default_client_name, "topic2", partition1, "key1", "value"]

      assert 0 <= partition1 && partition1 <= 32,
             "The partition should be in the range"

      :ok = Producer.produce_sync("topic2", "key1", "value")

      assert_receive [:produce_sync, @default_client_name, "topic2", ^partition1, "key1", "value"]
      # "Should receive the same partition for the same key"

      :ok = Producer.produce_sync("topic2", "key2", "value")
      assert_receive [:produce_sync, @default_client_name, "topic2", partition2, "key2", "value"]

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

      update_producer_config(@default_client_config_key, :partition_strategy, choose_partition)

      :ok = Producer.produce_sync("topic", "key", "value")
      assert_receive [:produce_sync, @default_client_name, "topic", 0, "key", "value"]

      :ok = Producer.produce_sync("topic", "key", "value")
      assert_receive [:produce_sync, @default_client_name, "topic", 0, "key", "value"]
    end

    test "does not use a selection strategy when given a direct partition" do
      :ok = Producer.produce_sync("topic", 0, "key", "value")
      assert_receive [:produce_sync, @default_client_name, "topic", 0, "key", "value"]

      :ok = Producer.produce_sync("topic", 0, "key", "value")
      assert_receive [:produce_sync, @default_client_name, "topic", 0, "key", "value"]
    end
  end

  defp update_producer_config(config_key, key, value) do
    producers_config = Application.get_env(:kaffe, :producers)
    Application.put_env(:kaffe, :producers, put_in(producers_config, [config_key, key], value))
    on_exit(fn -> Application.put_env(:kaffe, :producers, producers_config) end)
  end
end
