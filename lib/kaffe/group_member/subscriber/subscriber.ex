defmodule Kaffe.Subscriber do
  @moduledoc """
  Consume messages from a single partition of a single Kafka topic.
  
  Assignments are received from a group consumer member, `Kaffe.GroupMember`.

  Messages are delegated to `Kaffe.Worker`.

  The result from the worker is expected to be `:ok` and anything else
  will be an error.

  See: https://github.com/klarna/brucke/blob/master/src/brucke_member.erl
  """

  use GenServer

  alias Kaffe.Worker

  require Logger

  require Record
  import Record, only: [defrecord: 2, extract: 2]
  defrecord :kafka_message_set, extract(:kafka_message_set, from_lib: "brod/include/brod.hrl")
  defrecord :kafka_message, extract(:kafka_message, from_lib: "brod/include/brod.hrl")

  defmodule State do
    defstruct subscriber_pid: nil, group_coordinator_pid: nil, gen_id: nil, worker_pid: nil,
      subscriber_name: nil, topic: nil, partition: nil, subscribe_ops: nil,
      ack_offset: nil, retries_remaining: nil
  end

  def subscribe(subscriber_name, group_coordinator_pid, worker_pid,
      gen_id, topic, partition, ops) do
    GenServer.start_link(__MODULE__, [subscriber_name, group_coordinator_pid, worker_pid,
        gen_id, topic, partition, ops])
  end

  def stop(subscriber_pid) do
    Logger.debug "event#stopping=#{inspect self()}"
    GenServer.stop(subscriber_pid)
  end

  def ack_messages(subscriber_pid) do
    GenServer.cast(subscriber_pid, {:ack_messages})
  end

  def init([subscriber_name, group_coordinator_pid, worker_pid,
      gen_id, topic, partition, ops]) do
    send(self(), {:subscribe_to_topic_partition})
    {:ok, %State{group_coordinator_pid: group_coordinator_pid,
            worker_pid: worker_pid, gen_id: gen_id,
            subscriber_name: subscriber_name, topic: topic, partition: partition, subscribe_ops: ops ++ subscriber_ops(),
            ack_offset: nil, retries_remaining: max_retries()}}
  end

  def handle_info({_pid, message_set}, state) do

    messages = message_set
    |> kafka_message_set
    |> Enum.into(%{})
    |> Map.get(:messages)
    |> Enum.map(fn (message) ->
      compile_message(message, state.topic, state.partition)
    end)

    offset = Enum.reduce(messages, 0, &max(&1.offset, &2))

    Logger.debug "Sending message set to worker: #{inspect state.worker_pid}"
    Worker.process_messages(state.worker_pid, messages)

    {:noreply, %{state | ack_offset: offset}}
  end
  def handle_info({:subscribe_to_topic_partition},
      %{subscriber_name: subscriber_name,
        topic: topic,
        partition: partition,
        subscribe_ops: ops} = state) do
    kafka().subscribe(subscriber_name, self(), topic, partition, ops)
    |> handle_subscribe(state)
  end
  def handle_info({'DOWN', _ref, _process, _pid, reason}, state) do
    Logger.warn "event#consumer_down=#{inspect self()} reason=#{inspect reason}"
    {:stop, :consumer_down, state}
  end

  def handle_cast({:ack_messages}, state) do

    # Update the offsets in the group
    :ok = group_coordinator().ack(state.group_coordinator_pid, state.gen_id,
        state.topic, state.partition, state.ack_offset)
    # Request more messages from the consumer
    :ok = kafka().consume_ack(state.subscriber_pid, state.ack_offset)

    {:noreply, state}
  end

  defp handle_subscribe({:ok, subscriber_pid}, state) do
    Logger.debug "Subscribe success: #{inspect subscriber_pid}"
    {:noreply, %{state | subscriber_pid: subscriber_pid}}
  end
  defp handle_subscribe({:error, reason}, %{retries_remaining: retries_remaining} = state)
      when retries_remaining > 0 do
    Logger.debug "Failed to subscribe with reason: #{inspect reason}, #{retries_remaining} retries remaining"
    Process.send_after(self(), {:subscribe_to_topic_partition}, retry_delay())
    {:noreply, %{state | retries_remaining: retries_remaining - 1}}
  end
  defp handle_subscribe({:error, reason}, state) do
    Logger.warn "event#subscribe_failed=#{inspect self()} reason=#{inspect reason}"
    {:stop, {:subscribe_failed, :retries_exceeded, reason}, state}
  end

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

  defp subscriber_ops do
    [max_bytes: Kaffe.Config.Consumer.configuration.max_bytes]
  end

  defp max_retries do
    Kaffe.Config.Consumer.configuration.subscriber_retries
  end

  defp retry_delay do
    Kaffe.Config.Consumer.configuration.subscriber_retry_delay_ms
  end

end