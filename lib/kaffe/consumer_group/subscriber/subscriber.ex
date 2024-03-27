defmodule Kaffe.Subscriber do
  @moduledoc """
  Consume messages from a single partition of a single Kafka topic.

  Assignments are received from a group consumer member, `Kaffe.GroupMember`.

  Messages are delegated to `Kaffe.Worker`. The worker is expected to cast back
  a response, at which time the stored offset will be acked back to Kafka.

  The options (`ops`) to `subscribe/7` may include the beginning offset
  using `:begin_offset`.

  The subscriber reads the following options out of the configuration:

      - `max_bytes` - The maximum number of message bytes to receive in a batch
      - `min_bytes` - The minimum number of message bytes to receive in a batch
      - `max_wait_time` - Maximum number of milliseconds broker will wait for `:min_bytes` of messages
          to be collected
      - `offset_reset_policy` - The native `auto.offset.reset` option,
          either `:reset_to_earliest` or `:reset_to_latest`.

  See: https://github.com/klarna/brucke/blob/master/src/brucke_member.erl
  Also: https://github.com/klarna/brod/blob/master/src/brod_consumer.erl
  """

  use GenServer

  require Logger
  require Record

  import Record, only: [defrecord: 2, extract: 2]

  defrecord :kafka_message_set, extract(:kafka_message_set, from_lib: "brod/include/brod.hrl")
  defrecord :kafka_message, extract(:kafka_message, from_lib: "brod/include/brod.hrl")

  defmodule State do
    defstruct subscriber_pid: nil,
              group_coordinator_pid: nil,
              gen_id: nil,
              worker_pid: nil,
              subscriber_name: nil,
              topic: nil,
              partition: nil,
              subscribe_ops: nil,
              retries_remaining: nil,
              config: nil
  end

  ## ==========================================================================
  ## Public API
  ## ==========================================================================
  def subscribe(subscriber_name, group_coordinator_pid, worker_pid, gen_id, topic, partition, ops, config) do
    GenServer.start_link(
      __MODULE__,
      [
        subscriber_name,
        group_coordinator_pid,
        worker_pid,
        gen_id,
        topic,
        partition,
        ops,
        config
      ],
      name: name(subscriber_name, topic, partition)
    )
  end

  def stop(subscriber_pid) do
    Logger.info("event#stopping=#{inspect(self())}")
    GenServer.stop(subscriber_pid)
  end

  def commit_offsets(subscriber_pid, topic, partition, generation_id, offset) do
    GenServer.cast(subscriber_pid, {:commit_offsets, topic, partition, generation_id, offset})
  end

  def request_more_messages(subscriber_pid, offset) do
    GenServer.cast(subscriber_pid, {:request_more_messages, offset})
  end

  ## ==========================================================================
  ## Public API
  ## ==========================================================================
  def init([subscriber_name, group_coordinator_pid, worker_pid, gen_id, topic, partition, ops, config]) do
    send(self(), {:subscribe_to_topic_partition})

    {:ok,
     %State{
       group_coordinator_pid: group_coordinator_pid,
       worker_pid: worker_pid,
       gen_id: gen_id,
       subscriber_name: subscriber_name,
       topic: topic,
       partition: partition,
       subscribe_ops: ops ++ subscriber_ops(config),
       retries_remaining: max_retries(config),
       config: config
     }}
  end

  def handle_info(
        {_pid, {:kafka_message_set, topic, partition, _high_wm_offset, _messages} = message_set},
        state
      ) do
    ^topic = state.topic
    ^partition = state.partition

    messages =
      message_set
      |> kafka_message_set
      |> Enum.into(%{})
      |> Map.get(:messages)
      |> Enum.map(fn message ->
        compile_message(message, state.topic, state.partition)
      end)

    Logger.debug("Sending #{Enum.count(messages)} messages to worker: #{inspect(state.worker_pid)}")

    worker().process_messages(state.worker_pid, self(), topic, partition, state.gen_id, messages)

    {:noreply, state}
  end

  def handle_info(
        {:subscribe_to_topic_partition},
        %{
          subscriber_name: subscriber_name,
          topic: topic,
          partition: partition,
          subscribe_ops: ops
        } = state
      ) do
    kafka().subscribe(subscriber_name, self(), topic, partition, ops)
    |> handle_subscribe(state)
  end

  def handle_info({_pid, {:kafka_fetch_error, topic, partition, code, reason} = error}, state) do
    Logger.info(
      "event#kafka_fetch_error=#{inspect(self())} topic=#{topic} partition=#{partition} code=#{inspect(code)} reason=#{
        inspect(reason)
      }"
    )

    {:stop, {:shutdown, error}, state}
  end

  def handle_info({:DOWN, _ref, _process, pid, reason}, %{subscriber_pid: subscriber_pid} = state)
      when pid == subscriber_pid do
    Logger.warning("event#consumer_down=#{inspect(self())} reason=#{inspect(reason)}")
    {:stop, {:shutdown, {:consumer_down, reason}}, state}
  end

  def handle_info(unknown, state) do
    Logger.warning("event#unknown_message=#{inspect(self())} reason=#{inspect(unknown)}")
    {:noreply, state}
  end

  def handle_cast({:commit_offsets, topic, partition, generation_id, offset}, state) do
    Logger.debug("event#commit_offsets topic=#{state.topic} partition=#{state.partition} offset=#{offset} generation=#{generation_id}")

    # Is this the ack we're looking for?
    ^topic = state.topic
    ^partition = state.partition
    ^generation_id = state.gen_id

    # Update the offsets in the group
    :ok =
      group_coordinator().ack(
        state.group_coordinator_pid,
        state.gen_id,
        state.topic,
        state.partition,
        offset
      )

    {:noreply, state}
  end

  def handle_cast({:request_more_messages, offset}, state) do
    Logger.debug("event#request_more_messages topic=#{state.topic} partition=#{state.partition} offset=#{offset}")

    :ok = kafka().consume_ack(state.subscriber_pid, offset)

    {:noreply, state}
  end

  defp handle_subscribe({:ok, subscriber_pid}, state) do
    Logger.debug("Subscribe success: #{inspect(subscriber_pid)}")
    Process.monitor(subscriber_pid)
    {:noreply, %{state | subscriber_pid: subscriber_pid}}
  end

  defp handle_subscribe({:error, reason}, %{retries_remaining: retries_remaining} = state) when retries_remaining > 0 do
    Logger.debug("Failed to subscribe with reason: #{inspect(reason)}, #{retries_remaining} retries remaining")

    Process.send_after(self(), {:subscribe_to_topic_partition}, retry_delay(state.config))
    {:noreply, %{state | retries_remaining: retries_remaining - 1}}
  end

  defp handle_subscribe({:error, reason}, state) do
    Logger.warning("event#subscribe_failed=#{inspect(self())} reason=#{inspect(reason)}")
    {:stop, {:subscribe_failed, :retries_exceeded, reason}, state}
  end

  ## ==========================================================================
  ## Public API
  ## ==========================================================================
  defp compile_message(msg, topic, partition) do
    Map.merge(%{topic: topic, partition: partition}, kafka_message_to_map(msg))
  end

  defp kafka_message_to_map(msg) do
    Enum.into(kafka_message(msg), %{})
  end

  defp kafka do
    Application.get_env(:kaffe, :kafka_mod, :brod)
  end

  defp group_coordinator do
    Application.get_env(:kaffe, :group_coordinator_mod, :brod_group_coordinator)
  end

  defp worker do
    Application.get_env(:kaffe, :worker_mod, Kaffe.Worker)
  end

  defp subscriber_ops(config) do
    [
      max_bytes: config.max_bytes,
      min_bytes: config.min_bytes,
      max_wait_time: config.max_wait_time,
      offset_reset_policy: config.offset_reset_policy
    ]
  end

  defp max_retries(config) do
    config.subscriber_retries
  end

  defp retry_delay(config) do
    config.subscriber_retry_delay_ms
  end

  defp name(subscriber_name, topic, partition) do
    :"#{__MODULE__}.#{subscriber_name}.#{topic}.#{partition}"
  end
end
