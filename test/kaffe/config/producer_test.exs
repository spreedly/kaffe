defmodule Kaffe.Config.ProducerTest do
  use ExUnit.Case, async: true

  describe "configuration/0" do
    test "correct settings are extracted" do
      expected = %{
        endpoints: [kafka: 9092],
        producer_config: [
          auto_start_producers: true,
          allow_topic_auto_creation: false,
          default_producer_config: [
            required_acks: -1,
            ack_timeout: 1000,
            partition_buffer_limit: 512,
            partition_onwire_limit: 1,
            max_batch_size: 1_048_576,
            max_retries: 3,
            retry_backoff_ms: 500,
            compression: :no_compression,
            min_compression_batch_size: 1024
          ]
        ],
        topics: ["kaffe-test"],
        client_name: :kaffe_producer_client,
        partition_strategy: :md5
      }

      assert Kaffe.Config.Producer.configuration() == expected
    end
  end
end
