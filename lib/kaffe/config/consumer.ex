defmodule Kaffe.Config.Consumer do
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

  def subscriber_name(config_key),
    do: config_get(config_key, :subscriber_name, consumer_group(config_key)) |> String.to_atom()

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
      auto_start_producers: false,
      allow_topic_auto_creation: false,
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
    |> Map.get(config_key)
    |> Keyword.fetch!(key)
  end

  def config_get(config_key, :subscriber_name, _default), do: config_key

  def config_get(config_key, key, default) do
    Application.get_env(:kaffe, :consumers)
    |> Map.get(config_key)
    |> Keyword.get(key, default)
  end

  def validate_configuration!() do
    if Application.get_env(:kaffe, :consumers) == nil do
      old_config = Application.get_env(:kaffe, :consumer) || []
      subscriber_name = old_config |> Keyword.get(:subscriber_name, "subscriber_name")

      raise("""
      UPDATE CONSUMERS CONFIG:

      Set :kaffe, :consumers to a map with subscriber names as keys and config as values.
      For example:

      config :kaffe,
        consumers: %{
          #{inspect(subscriber_name)} => #{inspect(old_config)}
        }
      """)
    end
  end
end
