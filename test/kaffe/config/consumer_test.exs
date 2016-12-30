defmodule Kaffe.Config.ConsumerTest do
  use ExUnit.Case, async: true

  describe "configuration/0" do
    test "correct settings are extracted" do
      expected = %{
        endpoints: [kafka_test: 9092],
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
        ],
        message_handler: SilentMessage,
        async_message_ack: false,
      }

      assert Kaffe.Config.Consumer.configuration == expected
    end
  end
end
