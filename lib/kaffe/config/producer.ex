defmodule Kaffe.Config.Producer do
  def configuration do
    %{
      endpoints: endpoints(),
      producer_config: client_producer_config(),
      client_name: config_get(:client_name, :kaffe_producer_client),
      topics: producer_topics(),
      partition_strategy: config_get(:partition_strategy, :md5)
    }
  end

  def producer_topics, do: config_get!(:topics)

  def endpoints do
    case heroku_kafka?() do
      true -> Kaffe.Config.heroku_kafka_endpoints()
      false -> config_get!(:endpoints)
    end
  end

  def client_producer_config do
    default_client_producer_config() ++ maybe_heroku_kafka_ssl()
  end

  def maybe_heroku_kafka_ssl do
    case heroku_kafka?() do
      true -> Kaffe.Config.ssl_config()
      false -> []
    end
  end

  def default_client_producer_config do
    [
      auto_start_producers: true,
      allow_topic_auto_creation: false,
      default_producer_config: [
        required_acks: config_get(:required_acks, -1),
        ack_timeout: config_get(:ack_timeout, 1000),
        partition_buffer_limit: config_get(:partition_buffer_limit, 512),
        partition_onwire_limit: config_get(:partition_onwire_limit, 1),
        max_batch_size: config_get(:max_batch_size, 1_048_576),
        max_retries: config_get(:max_retries, 3),
        retry_backoff_ms: config_get(:retry_backoff_ms, 500),
        compression: config_get(:compression, :no_compression),
        min_compression_batch_size: config_get(:min_compression_batch_size, 1024)
      ]
    ]
  end

  def heroku_kafka? do
    config_get(:heroku_kafka_env, false)
  end

  def config_get!(key) do
    Application.get_env(:kaffe, :producer)
    |> Keyword.fetch!(key)
  end

  def config_get(key, default) do
    Application.get_env(:kaffe, :producer)
    |> Keyword.get(key, default)
  end
end
