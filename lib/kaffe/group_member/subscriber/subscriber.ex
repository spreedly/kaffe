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
      topic: nil, partition: nil, ack_offset: nil
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
    {:ok, subscriber_pid} = kafka().subscribe(subscriber_name, self(), topic, partition,
      ops ++ subscriber_ops())
    {:ok, %State{subscriber_pid: subscriber_pid, group_coordinator_pid: group_coordinator_pid,
            worker_pid: worker_pid, gen_id: gen_id, topic: topic, partition: partition,
            ack_offset: nil}}
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
  def handle_info({'DOWN', _ref, _process, _pid, reason}, _state) do
    Logger.warn "event#down=#{inspect self()} reason=#{inspect reason}"
  end

  def handle_cast({:ack_messages}, state) do

    # Update the offsets in the group
    :ok = group_coordinator().ack(state.group_coordinator_pid, state.gen_id,
        state.topic, state.partition, state.ack_offset)
    # Request more messages from the consumer
    :ok = kafka().consume_ack(state.subscriber_pid, state.ack_offset)

    {:noreply, state}
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

end