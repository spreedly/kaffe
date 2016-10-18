defmodule Kaffe.Consumer do
  @moduledoc """
  Consume messages from Kafka and pass to a given local module.
  """

  @behaviour :brod_group_subscriber

  require Record
  import Record, only: [defrecord: 2, extract: 2]
  defrecord :kafka_message, extract(:kafka_message, from_lib: "brod/include/brod.hrl")

  defmodule State do
    @moduledoc """
    Running state for the consumer.

    - `message_handler`: The module to call with each Kafka message
    - `async`: How to acknowledge Kafka messages as processed
    """
    defstruct message_handler: nil, async: false
  end

  ## -------------------------------------------------------------------------
  ## public api
  ## -------------------------------------------------------------------------

  @doc """
  Start a Kafka consumer

    - `client`: the id of an active brod client to use for consuming
    - `consumer_group`: the consumer group id (should be unique to your app)
    - `topics`: the list of Kafka topics to consume
    - `message_handler`: the module that will be called for each Kafka message
    - `async`: if false then Kafka messages will be acknowledged after handling is complete

  Note: If `async` is true then you'll need call `ack/4` to acknowledge offsets per topic and partition after messages are processed.
  """
  def start_link(client, consumer_group, topics, message_handler, async) do
    group_config = [offset_commit_policy: :commit_to_kafka_v2, offset_commit_interval_seconds: 5]
    consumer_config = [begin_offset: :earliest]
    init_args = [message_handler, async]
    :brod.start_link_group_subscriber(
      client, consumer_group, topics, group_config, consumer_config, __MODULE__, init_args)
  end

  @doc """
  Commit the offset to Kafka for the topic/partition and group consumer.

  You can either pass the pid of the running Consumer or the group consumer id.

  e.g.

  ```
  Kaffe.Consumer.ack(pid, "commitlog", 0, 124)

  Kaffe.Consumer.ack("index-consumer-group", "commitlog", 0, 124)
  ```
  """
  def ack(pid, topic, partition, offset) when is_pid(pid) do
    :brod_group_subscriber.ack(pid, topic, partition, offset)
  end

  def ack(group_consumer, topic, partition, offset) do
    group_consumer
    |> String.to_atom
    |> :brod_group_subscriber.ack(topic, partition, offset)
  end

  ## -------------------------------------------------------------------------
  ## callbacks
  ## -------------------------------------------------------------------------

  @doc """
  Initialize the consumer loop.
  """
  def init(consumer_group, [message_handler, async]) do
    Process.register(self, String.to_atom(consumer_group))
    {:ok, %Kaffe.Consumer.State{message_handler: message_handler, async: async}}
  end

  @doc """
  Call the message handler with the Kafka message.

  If `async` is true you'll need to manually call `ack/4` to acknowledge messages as processed per topic and partition.
  """
  def handle_message(topic, partition, msg, %{async: false, message_handler: handler} = state) do
    :ok = apply(handler, :handle_message, [compile_message(msg, topic, partition)])
    {:ok, :ack, state}
  end

  def handle_message(topic, partition, msg, %{async: true, message_handler: handler} = state) do
    :ok = apply(handler, :handle_message, [compile_message(msg, topic, partition)])
    {:ok, state}
  end

  ## -------------------------------------------------------------------------
  ## internal functions
  ## -------------------------------------------------------------------------

  defp compile_message(msg, topic, partition) do
    Map.merge(%{topic: topic, partition: partition}, kafka_message_to_map(msg))
  end

  defp kafka_message_to_map(msg) do
    Enum.into(kafka_message(msg), %{})
  end
end
