defmodule Kaffe.Worker do
  @moduledoc """

  A worker receives messages for a single topic partition.

  Processing the message set is delegated to the configured message
  handler. It is responsible for any error handling. The message handler
  must define a `handle_messages` function (*note* the pluralization!)
  to accept a list of messages.

  The result of `handle_messages` is sent back to the subscriber.
  """

  require Logger

  def start_link(message_handler, subscriber_name, worker_name) do
    GenServer.start_link(__MODULE__, [message_handler, worker_name],
        name: name(subscriber_name, worker_name))
  end

  def init([message_handler, worker_name]) do
    Logger.info "event#starting=#{__MODULE__} name=#{worker_name}"
    {:ok, %{message_handler: message_handler, worker_name: worker_name}}
  end

  def process_messages(pid, subscriber_pid, topic, partition, generation_id, messages) do
    GenServer.cast(pid, {:process_messages, subscriber_pid, topic, partition, generation_id, messages})
  end

  def handle_cast({:process_messages, subscriber_pid, topic, partition, generation_id, messages},
      %{message_handler: message_handler} = state) do

    case apply(message_handler, :handle_messages, [messages]) do
      :ok ->
        offset = List.last(messages).offset
        subscriber().commit_offsets(subscriber_pid, topic, partition, generation_id, offset)
        subscriber().request_more_messages(subscriber_pid, offset)
      {:ok, :no_commit} ->
        offset = List.last(messages).offset
        subscriber().request_more_messages(subscriber_pid, offset)
      {:ok, offset} ->
        subscriber().commit_offsets(subscriber_pid, topic, partition, generation_id, offset)
        subscriber().request_more_messages(subscriber_pid, offset)
    end

    {:noreply, state}
  end

  def terminate(reason, _state) do
    Logger.info "event#terminate=#{inspect self()} reason=#{inspect reason}"
  end

  defp name(subscriber_name, worker_name) do
    :"kaffe_#{subscriber_name}_#{worker_name}"
  end

  defp subscriber do
    Application.get_env(:kaffe, :subscriber_mod, Kaffe.Subscriber)
  end

end
