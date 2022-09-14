defmodule Kaffe.Config.Consumer do
  import Kaffe.Config, only: [heroku_kafka_endpoints: 0, parse_endpoints: 1]

  def configuration(idx) do
    %{
      endpoints: endpoints(idx),
      subscriber_name: subscriber_name(idx),
      consumer_group: consumer_group(idx),
      topics: topics(idx),
      group_config: consumer_group_config(idx),
      consumer_config: client_consumer_config(idx),
      message_handler: message_handler(idx),
      async_message_ack: async_message_ack(idx),
      rebalance_delay_ms: rebalance_delay_ms(idx),
      max_bytes: max_bytes(idx),
      min_bytes: min_bytes(idx),
      max_wait_time: max_wait_time(idx),
      subscriber_retries: subscriber_retries(idx),
      subscriber_retry_delay_ms: subscriber_retry_delay_ms(idx),
      offset_reset_policy: offset_reset_policy(idx),
      worker_allocation_strategy: worker_allocation_strategy(idx),
      client_down_retry_expire: client_down_retry_expire(idx)
    }
  end

  def consumer_group(idx), do: config_get!(idx, :consumer_group)

  def subscriber_name(idx), do: config_get(idx, :subscriber_name, consumer_group(idx)) |> String.to_atom()

  def topics(idx), do: config_get!(idx, :topics)

  def message_handler(idx), do: config_get!(idx, :message_handler)

  def async_message_ack(idx), do: config_get(idx, :async_message_ack, false)

  def endpoints(idx) do
    if heroku_kafka?(idx) do
      heroku_kafka_endpoints()
    else
      parse_endpoints(config_get!(idx, :endpoints))
    end
  end

  def consumer_group_config(idx) do
    [
      offset_commit_policy: :commit_to_kafka_v2,
      offset_commit_interval_seconds: config_get(idx, :offset_commit_interval_seconds, 5)
    ]
  end

  def rebalance_delay_ms(idx) do
    config_get(idx, :rebalance_delay_ms, 10_000)
  end

  def max_bytes(idx) do
    config_get(idx, :max_bytes, 1_000_000)
  end

  def min_bytes(idx) do
    config_get(idx, :min_bytes, 0)
  end

  def max_wait_time(idx) do
    config_get(idx, :max_wait_time, 10_000)
  end

  def subscriber_retries(idx) do
    config_get(idx, :subscriber_retries, 5)
  end

  def subscriber_retry_delay_ms(idx) do
    config_get(idx, :subscriber_retry_delay_ms, 5_000)
  end

  def client_consumer_config(idx) do
    default_client_consumer_config(idx) ++ maybe_heroku_kafka_ssl(idx) ++ sasl_options(idx) ++ ssl_options(idx)
  end

  def sasl_options(idx) do
    idx
    |> config_get(:sasl, %{})
    |> Kaffe.Config.sasl_config()
  end

  def ssl_options(idx) do
    idx
    |> config_get(:ssl, false)
    |> Kaffe.Config.ssl_config()
  end

  def default_client_consumer_config(idx) do
    [
      auto_start_producers: false,
      allow_topic_auto_creation: false,
      begin_offset: begin_offset(idx)
    ]
  end

  def begin_offset(idx) do
    case config_get(idx, :start_with_earliest_message, false) do
      true -> :earliest
      false -> -1
    end
  end

  def offset_reset_policy(idx) do
    config_get(idx, :offset_reset_policy, :reset_by_subscriber)
  end

  def worker_allocation_strategy(idx) do
    config_get(idx, :worker_allocation_strategy, :worker_per_partition)
  end

  def client_down_retry_expire(idx) do
    config_get(idx, :client_down_retry_expire, 30_000)
  end

  def maybe_heroku_kafka_ssl(idx) do
    case heroku_kafka?(idx) do
      true -> Kaffe.Config.ssl_config()
      false -> []
    end
  end

  def heroku_kafka?(idx) do
    config_get(idx, :heroku_kafka_env, false)
  end

  def config_get!(idx, :subscriber_name), do: idx

  def config_get!(idx, key) do
    Application.get_env(:kaffe, :consumers)
    |> Map.get(idx)
    |> Keyword.fetch!(key)
  end

  def config_get(idx, :subscriber_name, _default), do: idx

  def config_get(idx, key, default) do
    Application.get_env(:kaffe, :consumers)
    |> Map.get(idx)
    |> Keyword.get(key, default)
  end
end
