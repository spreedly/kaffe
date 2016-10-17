defmodule Kaffe.Producer do
  @moduledoc """
  Produce messages to Kafka.
  """

  @doc """
  Synchronously produce the given `key`, `value` to Kafka.

  - `client`: A running brod client
  - `topic`: The Kafka topic
  - `partition`: The Kafka partition
  - `key`: Kafka message key
  - `value`: Kafka message value
  """
  def produce_sync(client, topic, partition, key, value) do
    :brod.produce_sync(client, topic, partition, key, value)
  end

  @doc """
  Asynchronously produce the given `key`, `value` to Kafka.

  - `client`: A running brod client
  - `topic`: The Kafka topic
  - `partition`: The Kafka partition
  - `key`: Kafka message key
  - `value`: Kafka message value

  The function will immediately return `:ok` and will send an Elixir message to
  the calling process once the message is acknowledged by Kafka.
  """
  def produce_async(client, topic, partition, key, value) do
    :brod.produce(client, topic, partition, key, value)
  end
end
