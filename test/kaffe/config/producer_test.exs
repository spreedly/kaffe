defmodule Kaffe.Config.ProducerTest do
  use ExUnit.Case, async: true

  @default_producer :producer_name

  describe "configuration/0" do
    test "correct settings are extracted" do
      config = Application.get_env(:kaffe, :producers)

      no_sasl_config =
        config
        |> pop_in([@default_producer, :sasl])
        |> elem(1)

      Application.put_env(:kaffe, :producers, no_sasl_config)

      expected = %{
        endpoints: [{~c"kafka", 9092}],
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
        client_name: :kaffe_producer_client_producer_name,
        partition_strategy: :md5
      }

      on_exit(fn ->
        Application.put_env(:kaffe, :producers, config)
      end)

      assert Kaffe.Config.Producer.configuration(@default_producer) == expected
    end

    test "correct settings with sasl plain are extracted" do
      update_producer_config(@default_producer, :sasl, %{mechanism: :plain, login: "Alice", password: "ecilA"})

      expected = %{
        endpoints: [{~c"kafka", 9092}],
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
          ],
          sasl: {:plain, "Alice", "ecilA"}
        ],
        topics: ["kaffe-test"],
        client_name: :kaffe_producer_client_producer_name,
        partition_strategy: :md5
      }

      assert Kaffe.Config.Producer.configuration(@default_producer) == expected
    end

    test "correct settings are extracted for different producer clients" do
      config = Application.get_env(:kaffe, :producers)

      multiple_producers_config = [
        producer_1: [
          endpoints: [kafka1: 9092],
          topics: ["kaffe-test-1"],
          sasl: %{mechanism: :plain, login: "Alice", password: "ecilA"},
          ssl: true
        ],
        producer_2: [
          endpoints: [kafka2: 9092],
          topics: ["kaffe-test-2"],
          compression: :zstd
        ]
      ]

      Application.put_env(:kaffe, :producers, multiple_producers_config)

      expected_producer_config_1 = %{
        endpoints: [{~c"kafka1", 9092}],
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
          ],
          sasl: {:plain, "Alice", "ecilA"},
          ssl: true
        ],
        topics: ["kaffe-test-1"],
        client_name: :kaffe_producer_client_producer_1,
        partition_strategy: :md5
      }

      expected_producer_config_2 = %{
        endpoints: [{~c"kafka2", 9092}],
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
            compression: :zstd,
            min_compression_batch_size: 1024
          ]
        ],
        topics: ["kaffe-test-2"],
        client_name: :kaffe_producer_client_producer_2,
        partition_strategy: :md5
      }

      on_exit(fn ->
        Application.put_env(:kaffe, :producers, config)
      end)

      assert Kaffe.Config.Producer.configuration(:producer_1) == expected_producer_config_1
      assert Kaffe.Config.Producer.configuration(:producer_2) == expected_producer_config_2
    end

    test "the same settings are extracted from map and keyword configuration" do
      keyword_config = Application.get_env(:kaffe, :producers)
      assert is_list(keyword_config)
      assert keyword_config != []

      producer_config_from_keyword = Kaffe.Config.Producer.configuration(@default_producer)

      map_config = Map.new(keyword_config)

      Application.put_env(:kaffe, :producers, map_config)

      on_exit(fn ->
        Application.put_env(:kaffe, :producers, keyword_config)
      end)

      producer_config_from_map = Kaffe.Config.Producer.configuration(@default_producer)

      assert producer_config_from_keyword == producer_config_from_map
    end
  end

  describe "maybe_set_producers_env!/0" do
    test "returns :ok if producers are not configured" do
      producers_config = Application.get_env(:kaffe, :producers)

      Application.delete_env(:kaffe, :producers)
      Application.delete_env(:kaffe, :producer)

      on_exit(fn -> Application.put_env(:kaffe, :producers, producers_config) end)

      assert :ok == Kaffe.Config.Producer.maybe_set_producers_env!()

      assert is_nil(Application.get_env(:kaffe, :producer))
      assert is_nil(Application.get_env(:kaffe, :producers))
    end

    test "returns :ok if producers are configured with the :producers key" do
      producers_config = Application.get_env(:kaffe, :producers)
      assert is_list(producers_config)
      assert producers_config != []

      Application.delete_env(:kaffe, :producer)

      assert :ok == Kaffe.Config.Producer.maybe_set_producers_env!()

      assert is_nil(Application.get_env(:kaffe, :producer))
      assert Application.get_env(:kaffe, :producers) == producers_config
    end

    test "returns :ok and sets :kaffe, :producers if producer is configured with the :producer key" do
      producers_config = Application.get_env(:kaffe, :producers)
      producer_config = producers_config |> Keyword.values() |> List.first()

      Application.delete_env(:kaffe, :producers)
      Application.put_env(:kaffe, :producer, producer_config)

      on_exit(fn ->
        Application.put_env(:kaffe, :producers, producers_config)
        Application.delete_env(:kaffe, :producer)
      end)

      assert :ok == Kaffe.Config.Producer.maybe_set_producers_env!()

      assert Application.get_env(:kaffe, :producer) == producer_config
      assert Application.get_env(:kaffe, :producers) == [producer: producer_config]
    end

    test "logs message if :kaffe, :producers was set with configuration from :kaffe, :producer" do
      producers_config = Application.get_env(:kaffe, :producers)
      producer_config = producers_config |> Keyword.values() |> List.first()

      Application.delete_env(:kaffe, :producers)
      Application.put_env(:kaffe, :producer, producer_config)

      on_exit(fn ->
        Application.put_env(:kaffe, :producers, producers_config)
        Application.delete_env(:kaffe, :producer)
      end)

      logs =
        ExUnit.CaptureLog.capture_log(fn ->
          assert :ok == Kaffe.Config.Producer.maybe_set_producers_env!()
        end)

      assert logs =~ """
             Configuration for single producer is specified in :kaffe, :producer.

             To ensure backward compatibility :kaffe, :producers was set to a map \
             with default producer name as the key and single producer config as the value:

             config :kaffe, producers: [producer: #{inspect(producer_config)}]
             """
    end

    test "raises error is both :producer and :producers keys are present" do
      producers_config = Application.get_env(:kaffe, :producers)
      producer_config = producers_config |> Keyword.values() |> List.first()

      Application.put_env(:kaffe, :producer, producer_config)

      on_exit(fn ->
        Application.delete_env(:kaffe, :producer)
      end)

      expected_message = """
      FOUND SINGLE PRODUCER AND MULTIPLE PRODUCERS CONFIG:

      Delete `:kaffe, :producers` or `:kaffe, :producer` configuration.
      """

      assert_raise RuntimeError, expected_message, fn ->
        Kaffe.Config.Producer.maybe_set_producers_env!()
      end
    end
  end

  test "string endpoints parsed correctly" do
    update_producer_config(@default_producer, :endpoints, "kafka:9092,localhost:9092")

    expected = %{
      endpoints: [{~c"kafka", 9092}, {~c"localhost", 9092}],
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
      client_name: :kaffe_producer_client_producer_name,
      partition_strategy: :md5
    }

    assert Kaffe.Config.Producer.configuration(@default_producer) == expected
  end

  test "adds ssl when true" do
    update_producer_config(@default_producer, :ssl, true)

    expected = %{
      endpoints: [{~c"kafka", 9092}],
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
        ],
        ssl: true
      ],
      topics: ["kaffe-test"],
      client_name: :kaffe_producer_client_producer_name,
      partition_strategy: :md5
    }

    assert Kaffe.Config.Producer.configuration(@default_producer) == expected
  end

  defp update_producer_config(config_key, key, value) do
    producers_config = Application.get_env(:kaffe, :producers)
    Application.put_env(:kaffe, :producers, put_in(producers_config, [config_key, key], value))
    on_exit(fn -> Application.put_env(:kaffe, :producers, producers_config) end)
  end
end
