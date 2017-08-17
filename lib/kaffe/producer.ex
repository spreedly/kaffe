defmodule Kaffe.Producer do
  @moduledoc """

  The producer pulls in values from the Kaffe producer configuration:

    - `heroku_kafka_env` - endpoints and SSL configuration will be pulled from ENV
    - `endpoints` - plaintext Kafka endpoints
    - `topics` - a list of Kafka topics to prep for producing
    - `partition_strategy` - the strategy to use when selecting the next partition.
      Default `:md5`.
      - `:md5`: provides even and deterministic distrbution of the messages over the available partitions based on an MD5 hash of the key
      - `:random` - Select a random partition
      - function - Pass a function as an argument that accepts five arguments and
        returns the partition number to use for the message
          - `topic, current_partition, partitions_count, key, value`

  Clients can also specify a partition directly when producing.

  Currently only synchronous production is supported.
  """

  @kafka Application.get_env(:kaffe, :kafka_mod, :brod)

  require Logger

  ## -------------------------------------------------------------------------
  ## public api
  ## -------------------------------------------------------------------------

  def start_producer_client do
    @kafka.start_client(config().endpoints, client_name(), config().producer_config)
  end

  @doc """
  Synchronously produce the `messages` to `topic`

  `messages` must be a list of `{key, value}` tuples

  Returns:

       * `:ok` on successfully producing each message
       * `{:error, reason}` for any error
  """
  def produce_sync(topic, message_list) when is_list(message_list) do
    produce_list(topic, message_list, global_partition_strategy())
  end

  @doc """
  Synchronously produce the given `key`/`value` to the first Kafka topic.

  This is a simpler way to produce if you've only given Producer a single topic
  for production and don't want to specify the topic for each call.

  Returns:

       * `:ok` on successfully producing the message
       * `{:error, reason}` for any error
  """
  def produce_sync(key, value) do
    topic = config().topics |> List.first
    produce(topic, key, value)
  end

  @doc """
  Synchronously produce the `message_list` to `topic`/`partition`

  `message_list` must be a list of `{key, value}` tuples

  Returns:

       * `:ok` on successfully producing each message
       * `{:error, reason}` for any error
  """
  def produce_sync(topic, partition, message_list) when is_list(message_list) do
    produce_list(topic, message_list, fn _, _, _, _ -> partition end)
  end

  @doc """
  Synchronously produce the `key`/`value` to `topic`

  See `produce_sync/2` for returns.
  """
  def produce_sync(topic, key, value) do
    produce(topic, key, value)
  end

  @doc """
  Synchronously produce the given `key`/`value` to the `topic`/`partition`

  See `produce_sync/2` for returns.
  """
  def produce_sync(topic, partition, key, value) do
    @kafka.produce_sync(client_name(), topic, partition, key, value)
  end

  ## -------------------------------------------------------------------------
  ## internal
  ## -------------------------------------------------------------------------

  defp produce_list(topic, message_list, partition_strategy) when is_list(message_list) do
    Logger.debug "event#produce_list topic=#{topic}"
    message_list
    |> group_by_partition(topic, partition_strategy)
    |> produce_list_to_topic(topic)
  end

  defp produce(topic, key, value) do
    {:ok, partitions_count} = @kafka.get_partitions_count(client_name(), topic)
    partition = choose_partition(topic, partitions_count, key, value, global_partition_strategy())
    Logger.debug "event#produce topic=#{topic} key=#{key} partitions_count=#{partitions_count} selected_partition=#{partition}"
    @kafka.produce_sync(client_name(), topic, partition, key, value)
  end

  defp group_by_partition(messages, topic, partition_strategy) do
    {:ok, partitions_count} = @kafka.get_partitions_count(client_name(), topic)
    messages
    |> Enum.group_by(fn ({key, message}) ->
      choose_partition(topic, partitions_count, key, message, partition_strategy)
    end)
  end

  defp produce_list_to_topic(message_list, topic) do
    message_list
    |> Enum.reduce_while(:ok, fn ({partition, messages}, :ok) ->
      Logger.debug "event#produce_list_to_topic topic=#{topic} partition=#{partition}"
      case @kafka.produce_sync(client_name(), topic, partition, "ignored", messages) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp choose_partition(_topic, partitions_count, _key, _value, :random) do
    Kaffe.PartitionSelector.random(partitions_count)
  end

  defp choose_partition(_topic, partitions_count, key, _value, :md5) do
    Kaffe.PartitionSelector.md5(key, partitions_count)
  end

  defp choose_partition(topic, partitions_count, key, value, fun) when is_function(fun) do
    fun.(topic, partitions_count, key, value)
  end

  defp client_name do
    config().client_name
  end

  defp global_partition_strategy do
    config().partition_strategy
  end

  defp config do
    Kaffe.Config.Producer.configuration
  end
end
