defmodule Kaffe.Config.ConsumerTest do
  use ExUnit.Case

  describe "configuration/0" do
    setup do
      consumer_config =
        Application.get_env(:kaffe, :consumer)
        |> Keyword.delete(:offset_reset_policy)
        |> Keyword.delete(:ssl)
        |> Keyword.put(:start_with_earliest_message, true)

      Application.put_env(:kaffe, :consumer, consumer_config)
    end

    test "correct settings are extracted" do
      no_sasl_config =
        :kaffe
        |> Application.get_env(:consumer)
        |> Keyword.delete(:sasl)

      Application.put_env(:kaffe, :consumer, no_sasl_config)

      expected = %{
        endpoints: [{'kafka', 9092}],
        subscriber_name: :"kaffe-test-group",
        consumer_group: "kaffe-test-group",
        topics: ["kaffe-test"],
        group_config: [
          offset_commit_policy: :commit_to_kafka_v2,
          offset_commit_interval_seconds: 10
        ],
        consumer_config: [
          auto_start_producers: false,
          allow_topic_auto_creation: true,
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

      assert Kaffe.Config.Consumer.configuration() == expected
    end

    test "string endpoints parsed correctly" do
      config = Application.get_env(:kaffe, :consumer)
      endpoints = Keyword.get(config, :endpoints)
      Application.put_env(:kaffe, :consumer, Keyword.put(config, :endpoints, "kafka:9092,localhost:9092"))

      expected = %{
        endpoints: [{'kafka', 9092}, {'localhost', 9092}],
        subscriber_name: :"kaffe-test-group",
        consumer_group: "kaffe-test-group",
        topics: ["kaffe-test"],
        group_config: [
          offset_commit_policy: :commit_to_kafka_v2,
          offset_commit_interval_seconds: 10
        ],
        consumer_config: [
          auto_start_producers: false,
          allow_topic_auto_creation: true,
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
        Application.put_env(:kaffe, :consumer, Keyword.put(config, :endpoints, endpoints))
      end)

      assert Kaffe.Config.Consumer.configuration() == expected
    end
  end

  test "correct settings with sasl plain are extracted" do
    config = Application.get_env(:kaffe, :consumer)
    sasl = Keyword.get(config, :sasl)
    sasl_config = Keyword.put(config, :sasl, %{mechanism: :plain, login: "Alice", password: "ecilA"})

    Application.put_env(:kaffe, :consumer, sasl_config)

    expected = %{
      endpoints: [{'kafka', 9092}],
      subscriber_name: :"kaffe-test-group",
      consumer_group: "kaffe-test-group",
      topics: ["kaffe-test"],
      group_config: [
        offset_commit_policy: :commit_to_kafka_v2,
        offset_commit_interval_seconds: 10
      ],
      consumer_config: [
        auto_start_producers: false,
        allow_topic_auto_creation: true,
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
      Application.put_env(:kaffe, :consumer, Keyword.put(config, :sasl, sasl))
    end)

    assert Kaffe.Config.Consumer.configuration() == expected
  end

  test "correct settings with ssl are extracted" do
    config = Application.get_env(:kaffe, :consumer)
    ssl = Keyword.get(config, :ssl)
    ssl_config = Keyword.put(config, :ssl, true)

    Application.put_env(:kaffe, :consumer, ssl_config)

    expected = %{
      endpoints: [{'kafka', 9092}],
      subscriber_name: :"kaffe-test-group",
      consumer_group: "kaffe-test-group",
      topics: ["kaffe-test"],
      group_config: [
        offset_commit_policy: :commit_to_kafka_v2,
        offset_commit_interval_seconds: 10
      ],
      consumer_config: [
        auto_start_producers: false,
        allow_topic_auto_creation: true,
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
      Application.put_env(:kaffe, :consumer, Keyword.put(config, :ssl, ssl))
    end)

    assert Kaffe.Config.Consumer.configuration() == expected
  end

  describe "offset_reset_policy" do
    test "computes correctly from start_with_earliest_message == true" do
      consumer_config =
        Application.get_env(:kaffe, :consumer)
        |> Keyword.delete(:offset_reset_policy)

      Application.put_env(:kaffe, :consumer, consumer_config)

      assert Kaffe.Config.Consumer.configuration().offset_reset_policy == :reset_by_subscriber
    end
  end
end
