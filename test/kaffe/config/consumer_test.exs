defmodule Kaffe.Config.ConsumerTest do
  use ExUnit.Case

  def change_config(subscriber_name, update_fn) do
    config = Application.get_env(:kaffe, :consumers)[subscriber_name]
    config = update_fn.(config)
    Application.put_env(:kaffe, :consumers, %{subscriber_name => config})
  end

  describe "configuration/1" do
    setup do
      change_config("subscriber_name", fn(config) ->
        config
        |> Keyword.delete(:offset_reset_policy)
        |> Keyword.delete(:ssl)
        |> Keyword.put(:start_with_earliest_message, true)
      end)
    end

    test "correct settings are extracted" do
      sasl = Kaffe.Config.Consumer.config_get!("subscriber_name", :sasl)
      change_config("subscriber_name", fn(config) ->
        config |> Keyword.delete(:sasl)
      end)

      expected = %{
        endpoints: [{'kafka', 9092}],
        subscriber_name: :subscriber_name,
        consumer_group: "kaffe-test-group",
        topics: ["kaffe-test"],
        group_config: [
          offset_commit_policy: :commit_to_kafka_v2,
          offset_commit_interval_seconds: 10
        ],
        consumer_config: [
          auto_start_producers: false,
          allow_topic_auto_creation: false,
          begin_offset: :earliest
        ],
        message_handler: SilentMessage,
        async_message_ack: false,
        rebalance_delay_ms: 100,
        max_bytes: 10_000,
        min_bytes: 0,
        max_wait_time: 10_000,
        subscriber_retries: 1,
        subscriber_retry_delay_ms: 5,
        offset_reset_policy: :reset_by_subscriber,
        worker_allocation_strategy: :worker_per_partition,
        client_down_retry_expire: 15_000
      }

      on_exit(fn ->
        change_config("subscriber_name", fn(config) ->
          Keyword.put(config, :sasl, sasl)
        end)
      end)

      assert Kaffe.Config.Consumer.configuration("subscriber_name") == expected
    end

    test "string endpoints parsed correctly" do
      endpoints = Kaffe.Config.Consumer.config_get!("subscriber_name", :endpoints)
      change_config("subscriber_name", fn(config) ->
        config |> Keyword.put(:endpoints, "kafka:9092,localhost:9092")
      end)

      expected = %{
        endpoints: [{'kafka', 9092}, {'localhost', 9092}],
        subscriber_name: :subscriber_name,
        consumer_group: "kaffe-test-group",
        topics: ["kaffe-test"],
        group_config: [
          offset_commit_policy: :commit_to_kafka_v2,
          offset_commit_interval_seconds: 10
        ],
        consumer_config: [
          auto_start_producers: false,
          allow_topic_auto_creation: false,
          begin_offset: :earliest
        ],
        message_handler: SilentMessage,
        async_message_ack: false,
        rebalance_delay_ms: 100,
        max_bytes: 10_000,
        min_bytes: 0,
        max_wait_time: 10_000,
        subscriber_retries: 1,
        subscriber_retry_delay_ms: 5,
        offset_reset_policy: :reset_by_subscriber,
        worker_allocation_strategy: :worker_per_partition,
        client_down_retry_expire: 15_000
      }

      on_exit(fn ->
        change_config("subscriber_name", fn(config) ->
          Keyword.put(config, :endpoints, endpoints)
        end)
      end)

      assert Kaffe.Config.Consumer.configuration("subscriber_name") == expected
    end
  end

  test "correct settings with sasl plain are extracted" do
    sasl = Kaffe.Config.Consumer.config_get!("subscriber_name", :sasl)
    change_config("subscriber_name", fn(config) ->
      Keyword.put(config, :sasl, %{mechanism: :plain, login: "Alice", password: "ecilA"})
    end)

    expected = %{
      endpoints: [{'kafka', 9092}],
      subscriber_name: :subscriber_name,
      consumer_group: "kaffe-test-group",
      topics: ["kaffe-test"],
      group_config: [
        offset_commit_policy: :commit_to_kafka_v2,
        offset_commit_interval_seconds: 10
      ],
      consumer_config: [
        auto_start_producers: false,
        allow_topic_auto_creation: false,
        begin_offset: :earliest,
        sasl: {:plain, "Alice", "ecilA"}
      ],
      message_handler: SilentMessage,
      async_message_ack: false,
      rebalance_delay_ms: 100,
      max_bytes: 10_000,
      min_bytes: 0,
      max_wait_time: 10_000,
      subscriber_retries: 1,
      subscriber_retry_delay_ms: 5,
      offset_reset_policy: :reset_by_subscriber,
      worker_allocation_strategy: :worker_per_partition,
      client_down_retry_expire: 15_000
    }

    on_exit(fn ->
      change_config("subscriber_name", fn(config) ->
        Keyword.put(config, :sasl, sasl)
      end)
    end)

    assert Kaffe.Config.Consumer.configuration("subscriber_name") == expected
  end

  test "correct settings with ssl are extracted" do
    ssl = Kaffe.Config.Consumer.config_get("subscriber_name", :ssl, false)
    change_config("subscriber_name", fn(config) ->
      Keyword.put(config, :ssl, true)
    end)

    expected = %{
      endpoints: [{'kafka', 9092}],
      subscriber_name: :subscriber_name,
      consumer_group: "kaffe-test-group",
      topics: ["kaffe-test"],
      group_config: [
        offset_commit_policy: :commit_to_kafka_v2,
        offset_commit_interval_seconds: 10
      ],
      consumer_config: [
        auto_start_producers: false,
        allow_topic_auto_creation: false,
        begin_offset: :earliest,
        ssl: true
      ],
      message_handler: SilentMessage,
      async_message_ack: false,
      rebalance_delay_ms: 100,
      max_bytes: 10_000,
      min_bytes: 0,
      max_wait_time: 10_000,
      subscriber_retries: 1,
      subscriber_retry_delay_ms: 5,
      offset_reset_policy: :reset_by_subscriber,
      worker_allocation_strategy: :worker_per_partition,
      client_down_retry_expire: 15_000
    }

    on_exit(fn ->
      change_config("subscriber_name", fn(config) ->
        Keyword.put(config, :ssl, ssl)
      end)
    end)

    assert Kaffe.Config.Consumer.configuration("subscriber_name") == expected
  end

  describe "offset_reset_policy" do
    test "computes correctly from start_with_earliest_message == true" do
      change_config("subscriber_name", fn(config) ->
        config |> Keyword.delete(:offset_reset_policy)
      end)

      assert Kaffe.Config.Consumer.configuration("subscriber_name").offset_reset_policy == :reset_by_subscriber
    end
  end
end
