defmodule Kaffe.Config.Producer do
  @moduledoc """
  Configuration for Kaffe producers.

  * `:heroku_kafka_env` Endpoints and SSL configuration will be pulled from ENV

  * `:endpoints` The endpoints to produce to. Supports either host-port tuples (`[localhost: 9092]`,
    as an example), or urls.

  * `:topics` a list of Kafka topics to prep for producing

  * `:partition_strategy` The strategy to use when selecting the next partition. Defaults to `:md5`.
    * `:md5`: provides even and deterministic distrbution of the messages over the available partitions based
      on an MD5 hash of the key
    * `:random` - Select a random partition
    * function - Pass a function as an argument that accepts five arguments and returns the partition number
      to use for the message
        * The args are `topic, current_partition, partitions_count, key, value`

  * `:auto_start_producers` If true, `brod` client will spawn a producer automatically when user is trying
    to call produce but did not call brod:start_producer explicitly. Defaults to true. See `brod` documentation
    for more details.

  * `:allow_topic_auto_creation` By default, `brod` respects what is configured in the broker about topic auto-creation.
    i.e. whether `auto.create.topics.enable` is set in the broker configuration. However if `allow_topic_auto_creation`
    is set to false in client config, brod will avoid sending metadata requests that may cause an auto-creation of the
    topic regardless of what broker config is. Defaults to false. See `brod` for more details.

  * `:required_acks` How many acknowledgements the kafka broker should receive from the clustered replicas before
    acking producer. Defaults to -1. See brod for more details.
     * 0: the broker will not send any response (this is the only case where the broker will not reply to a request)
     * 1: The leader will wait the data is written to the local log before sending a response.
     * -1: If it is -1 the broker will block until the message is committed by all in sync replicas before acking.

  * `:ack_timeout` Maximum time in milliseconds the broker can await the receipt of the number of
    acknowledgements in `required_acks'. The timeout is not an exact limit on the request time. Defaults to 1000.
    See `brod` for more details.

  * `:partition_buffer_limit` How many requests (per-partition) can be buffered without blocking the caller.
    Defaults to 512. See `brod` for more details.

  * `:partition_onwire_limit`  How many message sets (per-partition) can be sent to kafka broker asynchronously
    before receiving ACKs from broker. Setting a number greater than 1 may cause messages being persisted in an
    order different from the order they were produced. Defaults to 1. See `brod` for more details.

  * `:max_batch_size` The number of bytes to batch produced messages to brokers in case callers are producing faster
    than brokers can handle. If compression is enabled, care should be taken when picking the max batch size, because
    a compressed batch will be produced as one message and this message might be larger than 'max.message.bytes'
    in kafka config (or topic config). See `brod` for more details.

  * `:max_retries` The number of times to retry message production. Defaults to 3. See `brod` for more details.

  * `:retry_backoff_ms` The number of ms to wait between retries. Defaults to 500. See `brod` for more details.

  * `:compression` The compression algorithm to use. Possible values are `:gzip`, `:no_compression`, or `:snappy`.
    Defaults to `:no_compression`.
  """

  import Kaffe.Config, only: [heroku_kafka_endpoints: 0, parse_endpoints: 1]

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
    if heroku_kafka?() do
      heroku_kafka_endpoints()
    else
      parse_endpoints(config_get!(:endpoints))
    end
  end

  def client_producer_config do
    default_client_producer_config() ++ maybe_heroku_kafka_ssl() ++ sasl_options() ++ ssl_options()
  end

  def sasl_options do
    :sasl
    |> config_get(%{})
    |> Kaffe.Config.sasl_config()
  end

  def maybe_heroku_kafka_ssl do
    case heroku_kafka?() do
      true -> Kaffe.Config.ssl_config()
      false -> []
    end
  end

  def ssl_options do
    :ssl
    |> config_get(false)
    |> Kaffe.Config.ssl_config()
  end

  def default_client_producer_config do
    [
      auto_start_producers: config_get(:auto_start_producers, true),
      allow_topic_auto_creation: config_get(:allow_topic_auto_creation, false),
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
