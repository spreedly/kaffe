defmodule Kaffe.Config.ConsumerTest do
  use ExUnit.Case

  describe "configuration/0" do

    setup do
      consumer_config = Application.get_env(:kaffe, :consumer)
      |> Keyword.delete(:offset_reset_policy)
      |> Keyword.put(:start_with_earliest_message, true)
      Application.put_env(:kaffe, :consumer, consumer_config)
    end

    test "correct settings are extracted" do
      expected = %{
        endpoints: [kafka: 9092],
        subscriber_name: :"kaffe-test-group",
        consumer_group: "kaffe-test-group",
        topics: ["kaffe-test"],
        group_config: [
          offset_commit_policy: :commit_to_kafka_v2,
          offset_commit_interval_seconds: 10,
        ],
        consumer_config: [
          auto_start_producers: false,
          allow_topic_auto_creation: false,
          begin_offset: :earliest,
          size_stat_window: 5
        ],
        message_handler: SilentMessage,
        async_message_ack: false,
        rebalance_delay_ms: 100,
        max_bytes: 10_000,
        subscriber_retries: 1,
        subscriber_retry_delay_ms: 5,
        offset_reset_policy: :reset_by_subscriber,
        worker_allocation_strategy: :worker_per_partition
      }

      assert Kaffe.Config.Consumer.configuration == expected
    end
  end

  describe "offset_reset_policy" do

    test "computes correctly from start_with_earliest_message == true" do
      consumer_config = Application.get_env(:kaffe, :consumer)
      |> Keyword.delete(:offset_reset_policy)
      Application.put_env(:kaffe, :consumer, consumer_config)

      assert Kaffe.Config.Consumer.configuration.offset_reset_policy == :reset_by_subscriber
    end

  end
end
