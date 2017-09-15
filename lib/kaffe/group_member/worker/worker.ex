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
    {:ok, %{message_handler: message_handler, worker_name: worker_name,
            max_messages: Kaffe.Config.Consumer.max_messages}}
  end

  def process_messages(pid, subscriber_pid, topic, partition, generation_id, messages) do
    GenServer.cast(pid, {:process_messages, subscriber_pid, topic, partition, generation_id, messages})
  end

  def handle_cast({:process_messages, subscriber_pid, topic, partition, generation_id, messages},
      %{message_handler: message_handler, max_messages: max_messages} = state) do

    process_batches!(messages, subscriber_pid, topic, partition, generation_id, message_handler, max_messages)

    {:noreply, state}
  end

  defp process_batches!([], _s, _t, _p, _g, _mh, _mm), do: nil
  defp process_batches!(batch, subscriber_pid, topic, partition, generation_id, message_handler, max_messages) do
    Logger.debug("Processing batch of #{length batch} messages in chunks of #{max_messages} for #{topic}/#{partition}")

    {messages, rest} = Enum.split(batch, max_messages)
    process_batch!(messages, subscriber_pid, topic, partition, generation_id,
      message_handler, max_messages)
    process_batches!(rest, subscriber_pid, topic, partition, generation_id, message_handler, max_messages)
  end

  defp process_batch!([], _s, _t, _p, _g, _mh, _mm), do: nil
  defp process_batch!(messages, subscriber_pid, topic, partition, generation_id, message_handler, max_messages) do
    Logger.debug("Processing chunk of #{length messages} for #{topic}/#{partition}")

    :ok = apply(message_handler, :handle_messages, [messages])
    offset = Enum.reduce(messages, 0, &max(&1.offset, &2))
    Logger.debug("Acking offset #{offset}")
    subscriber().ack_messages(subscriber_pid, topic, partition, generation_id, offset)
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
