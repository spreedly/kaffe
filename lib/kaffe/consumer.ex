defmodule Kaffe.Consumer do
  @moduledoc """
  Consume messages from Kafka and pass to a given local module.

  See `start_link/5` for details on how to start a Consumer process.

  As messages are consumed from Kafka they will be sent to your
  `handle_message/1` (sync) or `handle_message/2` (async) functions for
  processing in your system. Those functions _must_ return `:ok`.

  Kaffe.Consumer commits offsets to Kafka which is very reliable but not
  immediate. If your application restarts then it's highly likely you'll
  reconsume some messages, especially for a quickly moving topic. Be ready!
  """

  @behaviour :brod_group_subscriber

  require Record
  import Record, only: [defrecord: 2, extract: 2]
  defrecord :kafka_message, extract(:kafka_message, from_lib: "brod/include/brod.hrl")

  defmodule State do
    @moduledoc """
    Running state for the consumer.

    - `message_handler`: The module to call with each Kafka message
    - `async`: Kafka offset processing behavior
    """
    defstruct message_handler: nil, async: false
  end

  ## -------------------------------------------------------------------------
  ## public api
  ## -------------------------------------------------------------------------

  @doc """
  Start a Kafka consumer

  Required arguments:

    - `client`: the id of an active brod client to use for consuming
    - `consumer_group`: the consumer group id (should be unique to your app)
    - `topics`: the list of Kafka topics to consume
    - `message_handler`: the module that will be called for each Kafka message

  Optional:

    - `async`: if false then Kafka offsets will not be automatically
      acknowledged (default: false)

  Note: If `async` is true then you'll need to call `ack/2` to acknowledge
  Kafka messages as processed.

  Only use async processing if absolutely needed by your application's
  processing flow. With automatic (sync) acknowledgement then the message flow
  from Kaffe.Consumer has backpressure from your system. With manual (async)
  acknowledgement you will be able to process messages faster but will need to
  take on the burden of ensuring no message is lost.
  """
  def start_link(client, consumer_group, topics, message_handler, async \\ false) do
    group_config = [offset_commit_policy: :commit_to_kafka_v2, offset_commit_interval_seconds: 5]
    consumer_config = [begin_offset: :earliest]
    init_args = [message_handler, async]
    :brod.start_link_group_subscriber(
      client, consumer_group, topics, group_config, consumer_config, __MODULE__, init_args)
  end

  @doc """
  Acknowledge the Kafka message as processed.

  - `pid`: the pid your `handle_message/2` function was given as the first argument
  - `message`: the Kafka message your `handle_message/2` function was given as
    the second argument

  ```
  Kaffe.Consumer.ack(pid, message)
  ```
  """
  def ack(pid, %{topic: topic, partition: partition, offset: offset}) do
    :brod_group_subscriber.ack(pid, topic, partition, offset)
  end

  ## -------------------------------------------------------------------------
  ## callbacks
  ## -------------------------------------------------------------------------

  @doc """
  Initialize the consumer loop.
  """
  def init(_consumer_group, [message_handler, async]) do
    {:ok, %Kaffe.Consumer.State{message_handler: message_handler, async: async}}
  end

  @doc """
  Call the message handler with the restructured Kafka message.

  Kafka messages come from brod as an Erlang record. To make processing simpler
  for clients we convert that to an Elixir map. Since the consumer can
  subscribe to multiple topics with multiple partitions we also add the topic
  and partition as additional fields.

  After compiling the Kafka message your message handler module's
  `handle_message` function will be called.

  If `async` is false:

  - Your message handler module's `handle_message/1` function will be called
    with the message
  - The Consumer will block and wait for your `handle_message` function to
    complete and then automatically acknowledge the message as processed.

  If `async` is true:

  - Your message handler module's `handle_message/2` function will be called
    with the pid of the running Consumer process and the message.
  - Message intake on the Consumer will not wait for your `handle_message/2` to
    complete and will not automatically acknowledge the message as processed.
  - Once you've processed the message you will need to call
    `Kaffe.Consumer.ack/2` with the pid and message.
  """
  def handle_message(topic, partition, msg, %{async: false, message_handler: handler} = state) do
    :ok = apply(handler, :handle_message, [compile_message(msg, topic, partition)])
    {:ok, :ack, state}
  end

  def handle_message(topic, partition, msg, %{async: true, message_handler: handler} = state) do
    :ok = apply(handler, :handle_message, [self, compile_message(msg, topic, partition)])
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
