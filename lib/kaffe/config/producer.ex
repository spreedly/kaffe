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
    acknowledgements in `required_acks`. The timeout is not an exact limit on the request time. Defaults to 1000.
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

  require Logger

  @default_producer_config_key :producer

  def configuration(config_key) do
    %{
      endpoints: endpoints(config_key),
      producer_config: client_producer_config(config_key),
      client_name: client_name(config_key),
      topics: producer_topics(config_key),
      partition_strategy: partition_strategy(config_key)
    }
  end

  def producer_topics(config_key), do: config_get!(config_key, :topics)

  def endpoints(config_key) do
    if heroku_kafka?(config_key) do
      heroku_kafka_endpoints()
    else
      parse_endpoints(config_get!(config_key, :endpoints))
    end
  end

  def client_producer_config(config_key) do
    default_client_producer_config(config_key) ++
      maybe_heroku_kafka_ssl(config_key) ++
      sasl_options(config_key) ++
      ssl_options(config_key)
  end

  def client_name(config_key) do
    config_get(config_key, :client_name, :"kaffe_producer_client_#{config_key}")
  end

  def partition_strategy(config_key) do
    config_get(config_key, :partition_strategy, :md5)
  end

  def sasl_options(config_key) do
    config_key
    |> config_get(:sasl, %{})
    |> Kaffe.Config.sasl_config()
  end

  def maybe_heroku_kafka_ssl(config_key) do
    case heroku_kafka?(config_key) do
      true -> Kaffe.Config.ssl_config()
      false -> []
    end
  end

  def ssl_options(config_key) do
    config_key
    |> config_get(:ssl, false)
    |> Kaffe.Config.ssl_config()
  end

  def default_client_producer_config(config_key) do
    [
      auto_start_producers: config_get(config_key, :auto_start_producers, true),
      allow_topic_auto_creation: config_get(config_key, :allow_topic_auto_creation, false),
      default_producer_config: [
        required_acks: config_get(config_key, :required_acks, -1),
        ack_timeout: config_get(config_key, :ack_timeout, 1000),
        partition_buffer_limit: config_get(config_key, :partition_buffer_limit, 512),
        partition_onwire_limit: config_get(config_key, :partition_onwire_limit, 1),
        max_batch_size: config_get(config_key, :max_batch_size, 1_048_576),
        max_retries: config_get(config_key, :max_retries, 3),
        retry_backoff_ms: config_get(config_key, :retry_backoff_ms, 500),
        compression: config_get(config_key, :compression, :no_compression),
        min_compression_batch_size: config_get(config_key, :min_compression_batch_size, 1024)
      ]
    ]
  end

  def heroku_kafka?(config_key) do
    config_get(config_key, :heroku_kafka_env, false)
  end

  def config_get!(config_key, key) do
    :kaffe
    |> Application.get_env(:producers)
    |> Access.fetch!(config_key)
    |> Keyword.fetch!(key)
  end

  def config_get(config_key, key, default) do
    :kaffe
    |> Application.get_env(:producers)
    |> Access.fetch!(config_key)
    |> Keyword.get(key, default)
  end

  def list_config_keys do
    :kaffe
    |> Application.get_env(:producers)
    |> Enum.map(&elem(&1, 0))
  end

  @doc """
  Sets :kaffe, :producers application env if :kaffe, :producer is present.

  Provides backward compatibility between single producer and multiple producers.
  `:#{@default_producer_config_key}` config key is used for multiple producers config.
  """
  @spec maybe_set_producers_env!() :: :ok
  def maybe_set_producers_env! do
    single_config = Application.get_env(:kaffe, :producer) || []
    multiple_config = Application.get_env(:kaffe, :producers) || []

    if !Enum.empty?(single_config) and !Enum.empty?(multiple_config) do
      raise("""
      FOUND SINGLE PRODUCER AND MULTIPLE PRODUCERS CONFIG:

      Delete `:kaffe, :producers` or `:kaffe, :producer` configuration.
      """)
    end

    if !Enum.empty?(single_config) and Enum.empty?(multiple_config) do
      multiple_config = [{@default_producer_config_key, single_config}]

      Logger.info("""
      Configuration for single producer is specified in :kaffe, :producer.

      To ensure backward compatibility :kaffe, :producers was set to a map \
      with default producer name as the key and single producer config as the value:

      config :kaffe, producers: #{inspect(multiple_config)}
      """)

      Application.put_env(:kaffe, :producers, multiple_config)
    end

    :ok
  end
end
