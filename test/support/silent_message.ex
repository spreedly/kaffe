defmodule SilentMessage do
  def handle_message(_message), do: :ok
  def handle_message(_pid, _message), do: :ok
end
