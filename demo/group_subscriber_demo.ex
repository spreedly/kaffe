defmodule GroupSubscriberDemo do
  @behaviour :brod_group_subscriber
  @produce_delay_seconds 5

  require Logger
  require Record
  import Record, only: [defrecord: 2, extract: 2]
  defrecord :kafka_message, extract(:kafka_message, from_lib: "brod/include/brod.hrl")

  def bootstrap do
    bootstrap(@produce_delay_seconds)
  end

  def bootstrap(delay_seconds) do
    kafka_hosts = [kafka: 9092]
    topic = "brod-demo-group-subscriber-koc"
    {:ok, _} = Application.ensure_all_started(:brod)
    group_id = "brod-demo-group-subscriber-koc-consumer-group"
    topic_set = [topic]
    member_clients = [:"brod-demo-group-subscriber-koc-client-1", :"brod-demo-group-subscriber-koc-client-2"]
    :ok = bootstrap_subscribers(member_clients, kafka_hosts, group_id, topic_set)
    producer_client_id = :brod_demo_group_subscriber_koc
    :ok = :brod.start_client(kafka_hosts, producer_client_id, _client_config=[])
    :ok = :brod.start_producer(producer_client_id, topic, _producer_config=[])
    {:ok, partition_count} = :brod.get_partitions_count(producer_client_id, topic)
    :ok = spawn_producers(producer_client_id, topic, delay_seconds, partition_count)
    :ok
  end

  def init(_group_id, _callback_init_args = {client_id, topics}) do
    handlers = spawn_message_handlers(client_id, topics)
    {:ok, %{handlers: handlers}}
  end

  def handle_message(topic, partition, message, %{handlers: handlers} = state) do
    pid = handlers["#{topic}-#{partition}"]
    send pid, message
    {:ok, state}
  end

  def bootstrap_subscribers([], _kafka_hosts, _group_id, _topics), do: :ok
  def bootstrap_subscribers([client_id | rest], kafka_hosts, group_id, topics) do
    :ok = :brod.start_client(kafka_hosts, client_id, _client_config=[])
    group_config = [offset_commit_policy: :commit_to_kafka_v2, offset_commit_interval_seconds: 5]
    {:ok, _subscriber} = :brod.start_link_group_subscriber(
      client_id, group_id, topics, group_config,
      _consumer_config = [begin_offset: :earliest],
      _callback_module = __MODULE__,
      _callback_init_args = {client_id, topics})
    bootstrap_subscribers(rest, kafka_hosts, group_id, topics)
  end

  def spawn_producers(client_id, topic, delay_seconds, partition) when is_integer(partition) do
    partitions = :lists.seq(0, partition-1)
    spawn_producers(client_id, topic, delay_seconds, partitions)
  end
  def spawn_producers(client_id, topic, delay_seconds, [partition | partitions]) do
    spawn_link(fn -> producer_loop(client_id, topic, partition, delay_seconds, 0) end)
    spawn_producers(client_id, topic, delay_seconds, partitions)
  end
  def spawn_producers(_client_id, _topic, _delay_seconds, []), do: :ok

  def producer_loop(client_id, topic, partition, delay_seconds, seq_no) do
    kafka_value = "#{seq_no}"
    :ok = :brod.produce_sync(client_id, topic, partition, _key="", kafka_value)
    :timer.sleep(:timer.seconds(delay_seconds))
    producer_loop(client_id, topic, partition, delay_seconds, seq_no + 1)
  end

  def spawn_message_handlers(_client_id, []), do: %{}
  def spawn_message_handlers(client_id, [topic | rest]) do
    {:ok, partition_count} = :brod.get_partitions_count(client_id, topic)
    handlers = Enum.reduce :lists.seq(0, partition_count-1), %{}, fn partition, acc ->
      handler_pid = spawn_link(__MODULE__, :message_handler_loop, [topic, partition, self])
      Map.put(acc, "#{topic}-#{partition}", handler_pid)
    end
    Map.merge(handlers, spawn_message_handlers(client_id, rest))
  end

  def message_handler_loop(topic, partition, subscriber_pid) do
    receive do
      msg ->
        %{offset: offset, value: value} = Enum.into(kafka_message(msg), %{})
        Logger.info("#{inspect self} #{topic}-#{partition} Offset: #{offset}, Value: #{value}")
        :brod_group_subscriber.ack(subscriber_pid, topic, partition, offset)
        message_handler_loop(topic, partition, subscriber_pid)
    after
      1000 ->
        message_handler_loop(topic, partition, subscriber_pid)
    end
  end
end
