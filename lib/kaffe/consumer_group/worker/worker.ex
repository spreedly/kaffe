defmodule Kaffe.Worker do
  @moduledoc """
  A worker receives messages for a single topic for a single partition.

  Processing the message set is delegated to the configured message handler. It's
  responsible for any error handling as well. The message handler must define a
  `handle_messages` function (*note* the pluralization!) to accept a list of messages.

  The result of `handle_messages` is sent back to the subscriber. Additionally, the
  message handler should inform the subscriber on what to do with the offsets after
  processing the message set.
  """

  require Logger

  ## ==========================================================================
  ## Public API
  ## ==========================================================================
  def start_link(message_handler, subscriber_name, worker_name) do
    GenServer.start_link(__MODULE__, [message_handler, worker_name], name: name(subscriber_name, worker_name))
  end

  def process_messages(pid, subscriber_pid, topic, partition, generation_id, messages) do
    GenServer.cast(pid, {:process_messages, subscriber_pid, topic, partition, generation_id, messages})
  end

  ## ==========================================================================
  ## Callbacks
  ## ==========================================================================
  def init([message_handler, worker_name]) do
    Logger.info("event#starting=#{__MODULE__} name=#{worker_name}")
    {:ok, %{message_handler: message_handler, worker_name: worker_name}}
  end

  @doc """
  Entry point for processing a message set received by a subscriber.

  Note that the response from the message handler is what dictates how a
  subscriber should deal with the message offset. Depending on the situation,
  a message processor may not want to have it's most recent offsets committed.
  """
  def handle_cast(
        {:process_messages, subscriber_pid, topic, partition, generation_id, messages},
        %{message_handler: message_handler} = state
      ) do
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

  ## ==========================================================================
  ## Helpers
  ## ==========================================================================
  def terminate(reason, _state) do
    Logger.info("event#terminate=#{inspect(self())} reason=#{inspect(reason)}")
  end

  defp name(subscriber_name, worker_name) do
    :"#{__MODULE__}.#{subscriber_name}.#{worker_name}"
  end

  defp subscriber do
    Application.get_env(:kaffe, :subscriber_mod, Kaffe.Subscriber)
  end
end
