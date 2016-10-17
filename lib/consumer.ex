defmodule Consumer do
  @moduledoc """
  Consume messages from Kafka and pass to a given local module.
  """

  @behaviour :brod_topic_subscriber

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
  Start the Kafka topic subscriber.

  - `client`: the name of the running brod client to use for the subscription
  - `topic`: the Kafka topic to consume
  - `partition`: the topic partition to listen on (use `:all` for all partitions)
  - `consumer_config`: any specific brod consumer configuration values
  - `message_handler`: the project module that will be called for each Kafka message
  - `async`: if false then Kafka messages are acknowledged after handling is complete

  NOTE: Async message processing is not yet implemented.
  """
  def start_link(client, topic, partition, consumer_config, message_handler, async) do
    init_args = [message_handler, async]
    :brod_topic_subscriber.start_link(client, topic, partition, consumer_config, __MODULE__, init_args)
  end

  @doc """
  Initialize the topic subscriber.
  """
  def init(_topic, [message_handler, async]) do
    committed_offsets = []
    {:ok, committed_offsets, %Consumer.State{message_handler: message_handler, async: async}}
  end

  @doc """
  Call the message handler and acknowledge the message as processed.

  This is a synchonous Kafka acknowledgement. This function will block until
  the message_handler's work is finished.
  """
  def handle_message(_partition, msg, state = %{async: false}) do
    :ok = apply(state.message_handler, :handle_message, [kafka_message_to_map(msg)])
    {:ok, :ack, state}
  end

  defp kafka_message_to_map(msg) do
    Enum.into(kafka_message(msg), %{})
  end
end
