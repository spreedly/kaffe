defmodule Kaffe.Producer do
  @moduledoc """
  A GenServer for producing messages to a given Kafka topic.

  On startup Kaffe.Producer is given a brod client and a list of topics.
  """

  @name :kaffe_producer

  use GenServer

  defmodule State do
    @moduledoc """
    - `client`: the brod client to call for producing
    - `partition_strategy`: the strategy to determine the next partition
    - `partition_details`: a map of partition details keyed by topic
      - `total`: the number of partitions
      - `partition`: the next partition to produce to
    """
    defstruct client: nil, partition_strategy: nil, partition_details: %{}
  end

  ## -------------------------------------------------------------------------
  ## public api
  ## -------------------------------------------------------------------------

  @doc """
  Start a Kafka producer

  - `client`: a running brod client to use for producing
  - `topics` or `topic`: either a list of topics or a single topic to prep for
    producing
  - (optional) `strategy`: the strategy to use when selecting the next
    partition. Default `:round_robin`.

  Available partition strategies:

  - `:round_robin`: Cycle through the available partitions

  On initialization the producer will analyze the given topic(s) and determine
  their available partitions. That analysis will be paired with the given
  partition selection strategy to determine how the producer cycles through the
  partitions.

  Clients can also specify a partition directly when producing.
  """
  def start_link(client, topics, strategy \\ :round_robin)
  def start_link(client, topics, strategy) when is_list(topics) do
    GenServer.start_link(__MODULE__, [client, topics, strategy], name: @name)
  end
  def start_link(client, topic, strategy) do
    GenServer.start_link(__MODULE__, [client, [topic], strategy], name: @name)
  end

  @doc """
  Synchronously produce the given `key`/`value` to the first Kafka topic.

  This is a simpler way to produce if you've only given Producer a single topic
  for production and don't want to specify the topic whenever you call to
  produce.
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

  def init([client, topics, strategy]) do
    state = %Kaffe.Producer.State{
      client: client,
      partition_details: analyze(client, topics),
      partition_strategy: strategy}
    {:ok, state}
  end

  @doc """
  Sync produce the `key`/`value` to the default topic
  """
  def handle_call({:produce_sync, key, value}, _from, state) do
    topic_key = state.partition_details |> Map.keys |> List.first
    topic = Atom.to_string(topic_key)
    details = state.partition_details[topic_key]
    :brod.produce_sync(state.client, topic, details.partition, key, value)
    next_partition = next_partition(details, state.partition_strategy)
    {:reply, :ok, put_in(state.partition_details[topic_key].partition, next_partition)}
  end

  @doc """
  Sync produce the `key`/`value` to the given `topic`
  """
  def handle_call({:produce_sync, topic, key, value}, _from, state) do
    topic_key = String.to_atom(topic)
    details = state.partition_details[topic_key]
    :brod.produce_sync(state.client, topic, details.partition, key, value)
    next_partition = next_partition(details, state.partition_strategy)
    {:reply, :ok, put_in(state.partition_details[topic_key].partition, next_partition)}
  end

  @doc """
  Sync produce the `key`/`value` to the given `topic` and `partition`
  """
  def handle_call({:produce_sync, topic, partition, key, value}, _from, state) do
    :brod.produce_sync(state.client, topic, partition, key, value)
    {:reply, :ok, state}
  end

  ## -------------------------------------------------------------------------
  ## internal
  ## -------------------------------------------------------------------------

  defp analyze(client, topics) do
    topics
    |> Enum.reduce(%{}, fn(topic, details) ->
       {:ok, partition_count} = :brod.get_partitions_count(client, topic)
       Map.put(details, String.to_atom(topic), %{partition: 0, total: partition_count})
    end)
  end

  defp next_partition(%{partition: partition, total: total}, :round_robin) do
    Kaffe.PartitionSelector.round_robin(partition, total)
  end
end
