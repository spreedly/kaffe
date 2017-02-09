defmodule Kaffe.Config.ProducerTest do
  use ExUnit.Case, async: true

  describe "configuration/0" do
    test "correct settings are extracted" do
      expected = %{
        endpoints: [kafka_test: 9092],
        producer_config: [
          auto_start_producers: true,
          allow_topic_auto_creation: false,
          default_producer_config: [
            required_acks: -1
          ]
        ],
        topics: ["kaffe-test"],
        client_name: :kaffe_producer_client,
        partition_strategy: :md5,
      }

      assert Kaffe.Config.Producer.configuration == expected
    end
  end
end
