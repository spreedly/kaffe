defmodule Kaffe.Config.Consumer do
  @moduledoc """
  Configuration for Kaffe consumers.

  The configuration options for the `GroupMember` consumer are a superset of those for `Kaffe.Consumer`.

  The full list of supported config can be found below.

  * `:endpoints` The endpoints to consume from. Supports either host-port tuples (`[localhost: 9092]`,
    as an example), or urls.

  * `:heroku_kafka_env` Endpoints and SSL configuration will be pulled from ENV

  * `:consumer_group` The kafka consumer group the consumer is in. Should be unique to your app.

  * `:topics` A list of topics to consume from.

  * `:message_handler` The configured module to receive messages from the consumer.

  * `:async_message_ack` If false Kafka offset will automatically acknowledge after successful message parsing.
    If `async_message_ack` is true then you'll need to call `ack/2` to acknowledge Kafka messages as processed.
    Only use async processing if absolutely needed by your application's processing flow. With automatic (sync)
    acknowledgement then the message flow from `Kaffe.Consumer` has backpressure from your system. With
    manual (async) acknowledgement you will be able to process messages faster but will need to take on the burden
    of ensuring no messages are lost.

  * `:start_with_earliest_message` If true the worker will consume from the beginning of the topic when it first
    starts. This only affects consumer behavior before the consumer group starts recording its offsets in Kafka.

  * `:auto_start_producers` If true, `brod` client will spawn a producer automatically when user is trying
    to call produce but did not call brod:start_producer explicitly. Defaults to false. See `brod` documentation
    for more details.

  * `:allow_topic_auto_creation` By default, `brod` respects what is configured in the broker about topic auto-creation.
    i.e. whether `auto.create.topics.enable` is set in the broker configuration. However if `allow_topic_auto_creation`
    is set to false in client config, brod will avoid sending metadata requests that may cause an auto-creation of the
    topic regardless of what broker config is. Defaults to false. See `brod` for more details.

  * `:offset_commit_interval_seconds` Defines the time interval between two OffsetCommitRequest messages, defaulting
    to 5 seconds.

  * `:rebalance_delay_ms` The time to allow for rebalancing among workers. The default is 10,000, which should
    give the consumers time to rebalance when scaling.

  * `:max_bytes` Limits the number of message bytes received from Kafka for a particular topic subscriber. The
    default is 1MB. This parameter might need tuning depending on the number of partitions in the topics being
    read (there is one subscriber per topic per partition). For example, if you are reading from two topics, each
    with 32 partitions, there is the potential of 64MB in buffered messages at any one time.

  * `:min_bytes` Sets a minimum threshold for the number of bytes to fetch for a batch of messages. The default is 0MB.

  * `:max_wait_time` Sets the maximum number of milliseconds that the broker is allowed to collect min_bytes of
    messages in a batch of messages.

  * `:subscriber_retries` The number of times a subscriber will retry subscribing to a topic. Defaults to 5.

  * `:subscriber_retry_delay_ms` The ms a subscriber will delay connecting to a topic after a failure. Defaults to 5000.
    This only matters when `subscriber_retries` is greater than 0.

  * `:client_down_retry_expire` The amount of ms taken to attempt retries on a down client. Defaults to 30_000, and has
    exponential backoff (currently not configurable).

  * `:offset_reset_policy` Controls how the subscriber handles an expired offset. See the Kafka consumer
    option, [`auto.offset.reset`](https://kafka.apache.org/documentation/#newconsumerconfigs). Valid values for
    this option are:

  * `:reset_to_earliest` Reset to the earliest available offset.
  * `:reset_to_latest` Reset to the latest offset.
  * `:reset_by_subscriber` The subscriber receives the `OffsetOutOfRange` error.

  More information in the [Brod consumer](https://github.com/klarna/brod/blob/master/src/brod_consumer.erl).

  * `:worker_allocation_strategy` Controls how workers are allocated with respect to consumed topics and partitions.

  * `:worker_per_partition` The default (for backward compatibilty) and allocates a single worker per partition
      across topics. This is useful for managing concurrent processing of messages that may be received from any
      consumed topic.

  * `:worker_per_topic_partition` This strategy allocates a worker per topic partition. This means there will be
      a worker for every topic partition consumed. Unless you need to control concurrency across topics, you should
      use this strategy.
  """

  import Kaffe.Config, only: [heroku_kafka_endpoints: 0, parse_endpoints: 1]
  require Logger

  def configuration(config_key) do
    %{
      endpoints: endpoints(config_key),
      subscriber_name: subscriber_name(config_key),
      consumer_group: consumer_group(config_key),
      topics: topics(config_key),
      group_config: consumer_group_config(config_key),
      consumer_config: client_consumer_config(config_key),
      message_handler: message_handler(config_key),
      async_message_ack: async_message_ack(config_key),
      rebalance_delay_ms: rebalance_delay_ms(config_key),
      max_bytes: max_bytes(config_key),
      min_bytes: min_bytes(config_key),
      max_wait_time: max_wait_time(config_key),
      subscriber_retries: subscriber_retries(config_key),
      subscriber_retry_delay_ms: subscriber_retry_delay_ms(config_key),
      offset_reset_policy: offset_reset_policy(config_key),
      worker_allocation_strategy: worker_allocation_strategy(config_key),
      client_down_retry_expire: client_down_retry_expire(config_key)
    }
  end

  def consumer_group(config_key), do: config_get!(config_key, :consumer_group)

  def subscriber_name(config_key) do
    config_key
    |> config_get!(:subscriber_name)
    |> to_atom()
  end

  def topics(config_key), do: config_get!(config_key, :topics)

  def message_handler(config_key), do: config_get!(config_key, :message_handler)

  def async_message_ack(config_key), do: config_get(config_key, :async_message_ack, false)

  def endpoints(config_key) do
    if heroku_kafka?(config_key) do
      heroku_kafka_endpoints()
    else
      parse_endpoints(config_get!(config_key, :endpoints))
    end
  end

  def consumer_group_config(config_key) do
    [
      offset_commit_policy: :commit_to_kafka_v2,
      offset_commit_interval_seconds: config_get(config_key, :offset_commit_interval_seconds, 5)
    ]
  end

  def rebalance_delay_ms(config_key) do
    config_get(config_key, :rebalance_delay_ms, 10_000)
  end

  def max_bytes(config_key) do
    config_get(config_key, :max_bytes, 1_000_000)
  end

  def min_bytes(config_key) do
    config_get(config_key, :min_bytes, 0)
  end

  def max_wait_time(config_key) do
    config_get(config_key, :max_wait_time, 10_000)
  end

  def subscriber_retries(config_key) do
    config_get(config_key, :subscriber_retries, 5)
  end

  def subscriber_retry_delay_ms(config_key) do
    config_get(config_key, :subscriber_retry_delay_ms, 5_000)
  end

  def client_consumer_config(config_key) do
    default_client_consumer_config(config_key) ++
      maybe_heroku_kafka_ssl(config_key) ++ sasl_options(config_key) ++ ssl_options(config_key)
  end

  def sasl_options(config_key) do
    config_key
    |> config_get(:sasl, %{})
    |> Kaffe.Config.sasl_config()
  end

  def ssl_options(config_key) do
    config_key
    |> config_get(:ssl, false)
    |> Kaffe.Config.ssl_config()
  end

  def default_client_consumer_config(config_key) do
    [
      auto_start_producers: config_get(config_key, :auto_start_producers, false),
      allow_topic_auto_creation: config_get(config_key, :allow_topic_auto_creation, false),
      begin_offset: begin_offset(config_key)
    ]
  end

  def begin_offset(config_key) do
    case config_get(config_key, :start_with_earliest_message, false) do
      true -> :earliest
      false -> -1
    end
  end

  def offset_reset_policy(config_key) do
    config_get(config_key, :offset_reset_policy, :reset_by_subscriber)
  end

  def worker_allocation_strategy(config_key) do
    config_get(config_key, :worker_allocation_strategy, :worker_per_partition)
  end

  def client_down_retry_expire(config_key) do
    config_get(config_key, :client_down_retry_expire, 30_000)
  end

  def maybe_heroku_kafka_ssl(config_key) do
    case heroku_kafka?(config_key) do
      true -> Kaffe.Config.ssl_config()
      false -> []
    end
  end

  def heroku_kafka?(config_key) do
    config_get(config_key, :heroku_kafka_env, false)
  end

  def config_get!(config_key, :subscriber_name), do: config_key

  def config_get!(config_key, key) do
    Application.get_env(:kaffe, :consumers)
    |> Access.get(config_key)
    |> Keyword.fetch!(key)
  end

  def config_get(config_key, :subscriber_name, _default), do: config_key

  def config_get(config_key, key, default) do
    Application.get_env(:kaffe, :consumers)
    |> Access.get(config_key)
    |> Keyword.get(key, default)
  end

  def validate_configuration!() do
    if Application.get_env(:kaffe, :consumers) == nil do
      old_config = Application.get_env(:kaffe, :consumer) || []
      subscriber_name = old_config |> Keyword.get(:subscriber_name, :subscriber_name)

      raise("""
      UPDATE CONSUMERS CONFIG:

      Set :kaffe, :consumers to a keyword list with subscriber names as keys and config as values.
      For example:

      config :kaffe,
        consumers: [
          #{inspect(subscriber_name)} => #{inspect(old_config)}
        ]
      """)
    end
  end

  defp to_atom(val) when is_atom(val), do: val
  defp to_atom(val) when is_binary(val), do: String.to_atom(val)
end
