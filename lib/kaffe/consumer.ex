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

  @doc """
  Start a Kafka consumer

  - `client`: the name of the running brod client to use for the subscription
  - `topics`: the Kafka topics to consume
  - `partition`: the topic partition to listen on (use `:all` for all partitions)
  - `consumer_config`: any specific brod consumer configuration values
  - `message_handler`: the project module that will be called for each Kafka message
  - `async`: if false then Kafka messages are acknowledged after handling is complete

  NOTE: Async message processing is not yet implemented.
  """
  def start_link(client, consumer_group, topics, message_handler, async) do
    group_config = [offset_commit_policy: :commit_to_kafka_v2, offset_commit_interval_seconds: 5]
    consumer_config = [begin_offset: :earliest]
    init_args = [message_handler, async]
    :brod.start_link_group_subscriber(
      client, consumer_group, topics, group_config, consumer_config, __MODULE__, init_args)
  end

  @doc """
  Initialize the consumer loop.
  """
  def init(_topic, [message_handler, async]) do
    {:ok, %Kaffe.Consumer.State{message_handler: message_handler, async: async}}
  end

  @doc """
  Call the message handler and acknowledge the message as processed.

  This is a synchonous Kafka acknowledgement. This function will block until
  the message_handler's work is finished.
  """
  def handle_message(topic, partition, msg, %{async: false, message_handler: handler} = state) do
    :ok = apply(handler, :handle_message, [compile_message(msg, topic, partition)])
    {:ok, :ack, state}
  end

  defp compile_message(msg, topic, partition) do
    Map.merge(%{topic: topic, partition: partition}, kafka_message_to_map(msg))
  end

  defp kafka_message_to_map(msg) do
    Enum.into(kafka_message(msg), %{})
  end
end
