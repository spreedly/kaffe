defmodule TestBrodGroupSubscriber do
  def ack(_pid, _topic, _partition, _offset), do: :ok
end
