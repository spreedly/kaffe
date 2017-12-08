defmodule Kaffe.Config.Producer do
  def configuration do
    %{
      endpoints: endpoints(),
      producer_config: client_producer_config(),
      client_name: config_get(:client_name, :kaffe_producer_client),
      topics: producer_topics(),
      partition_strategy: config_get(:partition_strategy, :md5),
    }
  end

  def producer_topics, do: config_get!(:topics)

  def endpoints, do: Kaffe.Config.endpoints(config())

  def client_producer_config do
    default_client_producer_config() ++ Kaffe.Config.ssl(config())
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
        max_batch_size: config_get(:max_batch_size, 1048576),
        max_retries: config_get(:max_retries, 3),
        retry_backoff_ms: config_get(:retry_backoff_ms, 500),
        compression: config_get(:compression, :no_compression),
        min_compression_batch_size: config_get(:min_compression_batch_size, 1024)
      ]
    ]
  end

  def config, do: Application.get_env(:kaffe, :producer)

  def config_get!(key), do: Keyword.fetch!(config(), key)

  def config_get(key, default), do: Keyword.get(config(), key, default)

end
