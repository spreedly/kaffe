defmodule Kaffe.Producer do
  @moduledoc """
  A GenServer for producing messages to a given Kafka topic.
  """

  @name :kaffe_producer
  @kafka Application.get_env(:kaffe, :kafka_mod, :brod)

  use GenServer

  defmodule State do
    @moduledoc """
    - `client` - the brod client to call for producing
    - `topics` - a list of configured topics
    - `partition_strategy` - the strategy to determine the next partition
    - `partition_details` - a map of partition details keyed by topic
      - `total` - the number of partitions
      - `partition` - the next partition to produce to
    """
    defstruct client: nil, topics: nil, partition_strategy: nil, partition_details: %{}
  end

  ## -------------------------------------------------------------------------
  ## public api
  ## -------------------------------------------------------------------------

  @doc """
  Start a Kafka producer

  The producer pulls in values from the Kaffe producer configuration:

    - `heroku_kafka_env` - endpoints and SSL configuration will be pulled from ENV
    - `endpoints` - plaintext Kafka endpoints
    - `topics` - a list of Kafka topics to prep for producing
    - `partition_strategy` - the strategy to use when selecting the next partition.
      Default `:round_robin`.
      - `:round_robin` - Cycle through the available partitions
      - `:random` - Select a random partition
      - function - Pass a function as an argument that accepts five arguments and
        returns the partition number to use for the message
          - `topic, current_partition, partitions_count, key, value`

  On initialization the producer will analyze the given topic(s) and determine
  their available partitions. That analysis will be paired with the given
  partition selection strategy to select the partition per message.

  Clients can also specify a partition directly when producing.

  Currently only synchronous production is supported.
  """
  def start_link do
    config = Kaffe.Config.Producer.configuration
    GenServer.start_link(__MODULE__, [config], name: @name)
  end

  @doc """
  Synchronously produce the given `key`/`value` to the first Kafka topic.

  This is a simpler way to produce if you've only given Producer a single topic
  for production and don't want to specify the topic for each call.
  """
  def produce_sync(key, value) do
    GenServer.call(@name, {:produce_sync, key, value})
  end

  @doc """
  Synchronously produce the `key`/`value` to `topic`
  """
  def produce_sync(topic, key, value) do
    GenServer.call(@name, {:produce_sync, topic, key, value})
  end

  @doc """
  Synchronously produce the given `key`/`value` to the `topic`/`partition`
  """
  def produce_sync(topic, partition, key, value) do
    GenServer.call(@name, {:produce_sync, topic, partition, key, value})
  end

  ## -------------------------------------------------------------------------
  ## GenServer callbacks
  ## -------------------------------------------------------------------------

  def init([config]) do
    Kaffe.start_producer_client(config)
    state = %Kaffe.Producer.State{
      client: config.client_name,
      topics: config.topics,
      partition_details: analyze(config.client_name, config.topics),
      partition_strategy: config.partition_strategy}
    {:ok, state}
  end

  @doc """
  Sync produce the `key`/`value` to the default topic
  """
  def handle_call({:produce_sync, key, value}, _from, state) do
    topic = state.topics |> List.first
    {:ok, new_state} = produce(topic, key, value, state)
    {:reply, :ok, new_state}
  end

  @doc """
  Sync produce the `key`/`value` to the given `topic`
  """
  def handle_call({:produce_sync, topic, key, value}, _from, state) do
    {:ok, new_state} = produce(topic, key, value, state)
    {:reply, :ok, new_state}
  end

  @doc """
  Sync produce the `key`/`value` to the given `topic` and `partition`
  """
  def handle_call({:produce_sync, topic, partition, key, value}, _from, state) do
    @kafka.produce_sync(state.client, topic, partition, key, value)
    {:reply, :ok, state}
  end

  ## -------------------------------------------------------------------------
  ## internal
  ## -------------------------------------------------------------------------

  defp produce(topic, key, value, state) do
    topic_key = String.to_atom(topic)
    details = state.partition_details[topic_key]
    partition = choose_partition(
      topic, details.partition, details.total, key, value, state.partition_strategy)
    :ok = @kafka.produce_sync(state.client, topic, partition, key, value)
    {:ok, put_in(state.partition_details[topic_key].partition, partition)}
  end

  defp analyze(client, topics) do
    topics
    |> Enum.reduce(%{}, fn(topic, details) ->
       {:ok, partition_count} = @kafka.get_partitions_count(client, topic)
       Map.put(details, String.to_atom(topic), %{partition: nil, total: partition_count})
    end)
  end

  defp choose_partition(_topic, current_partition, partitions_count, _key, _value, :round_robin) do
    Kaffe.PartitionSelector.round_robin(current_partition, partitions_count)
  end

  defp choose_partition(_topic, _current_partition, partitions_count, _key, _value, :random) do
    Kaffe.PartitionSelector.random(partitions_count)
  end

  defp choose_partition(topic, current_partition, partitions_count, key, value, fun) when is_function(fun) do
    fun.(topic, current_partition, partitions_count, key, value)
  end
end
