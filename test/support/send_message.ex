defmodule SendMessage do
  def handle_message(message) do
    send self(), message
    :ok
  end

  def handle_message(pid, message) do
    send pid, message
    :ok
  end
end
