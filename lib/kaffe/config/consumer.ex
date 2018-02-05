defmodule Kaffe.Config.Consumer do
  def configuration do
    %{
      endpoints: endpoints(),
      subscriber_name: subscriber_name(),
      consumer_group: consumer_group(),
      topics: topics(),
      group_config: consumer_group_config(),
      consumer_config: client_consumer_config(),
      message_handler: message_handler(),
      async_message_ack: async_message_ack(),
      rebalance_delay_ms: rebalance_delay_ms(),
      max_bytes: max_bytes(),
      subscriber_retries: subscriber_retries(),
      subscriber_retry_delay_ms: subscriber_retry_delay_ms(),
      offset_reset_policy: offset_reset_policy(),
      worker_allocation_strategy: worker_allocation_strategy(),
    }
  end

  def consumer_group, do: config_get!(:consumer_group)

  def subscriber_name, do: config_get(:subscriber_name, consumer_group()) |> String.to_atom

  def topics, do: config_get!(:topics)

  def message_handler, do: config_get!(:message_handler)

  def async_message_ack, do: config_get(:async_message_ack, false)

  def endpoints do
    case heroku_kafka?() do
      true -> Kaffe.Config.heroku_kafka_endpoints()
      false -> config_get!(:endpoints)
    end
  end

  def consumer_group_config do
    [
      offset_commit_policy: :commit_to_kafka_v2,
      offset_commit_interval_seconds: config_get(:offset_commit_interval_seconds, 5),
    ]
  end

  def rebalance_delay_ms do
   config_get(:rebalance_delay_ms, 10_000)
  end

  def max_bytes do
   config_get(:max_bytes, 1_000_000)
  end

  def subscriber_retries do
   config_get(:subscriber_retries, 5)
  end

  def subscriber_retry_delay_ms do
   config_get(:subscriber_retry_delay_ms, 5_000)
  end

  def client_consumer_config do
    default_client_consumer_config()
    ++ maybe_heroku_kafka_ssl()
  end

  def default_client_consumer_config do
    [
      auto_start_producers: false,
      allow_topic_auto_creation: false,
      begin_offset: begin_offset(),
      size_stat_window: config_get(:size_stat_window, 5)
    ]
  end

  def begin_offset do
    case config_get(:start_with_earliest_message, false) do
      true -> :earliest
      false -> -1
    end
  end

  def offset_reset_policy do
    config_get(:offset_reset_policy, :reset_by_subscriber)
  end

  def worker_allocation_strategy do
    config_get(:worker_allocation_strategy, :worker_per_partition)
  end

  def maybe_heroku_kafka_ssl do
    case heroku_kafka?() do
      true -> Kaffe.Config.ssl_config()
      false -> []
    end
  end

  def heroku_kafka? do
    config_get(:heroku_kafka_env, false)
  end

  def config_get!(key) do
    Application.get_env(:kaffe, :consumer)
    |> Keyword.fetch!(key)
  end

  def config_get(key, default) do
    Application.get_env(:kaffe, :consumer)
    |> Keyword.get(key, default)
  end
end
