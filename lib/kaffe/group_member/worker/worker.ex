defmodule Kaffe.Worker do
  @moduledoc """
  A worker is assigned a single partition across topics for the client. This is
  so we effectively serialize the processing of any single key across topics.

  Processing the message set is delegated to the configured message handler. It
  is responsible for any error handling. The message handler must define a
  `handle_messages` function (*note* the pluralization!) to accept a list of
  messages.

  The result of handle message is sent back to the subscriber.
  """

  alias Kaffe.Subscriber

  require Logger

  def start_link(message_handler, partition) do
    GenServer.start_link(__MODULE__, [message_handler, partition], name: :"partition_worker_#{partition}")
  end

  def init([message_handler, partition]) do
    Logger.info "event#starting=#{inspect self()} source=#{__MODULE__}"
    {:ok, %{message_handler: message_handler,
            partition: partition}}
  end

  def process_messages(pid, messages) do
    GenServer.cast(pid, {:process_messages, self(), messages})
  end

  def handle_cast({:process_messages, subscriber_pid, messages},
      %{message_handler: message_handler} = state) do

    :ok = apply(message_handler, :handle_messages, [messages])
    Subscriber.ack_messages(subscriber_pid)

    {:noreply, state}
  end

  def terminate(reason, _state) do
    Logger.info "event#terminate=#{inspect self()} reason=#{inspect reason}"
  end

end