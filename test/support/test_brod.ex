defmodule TestBrod do
  use GenServer

  @test_partition_count Application.get_env(:kaffe, :test_partition_count)

  def start_link_group_subscriber(_client, _consumer_group, _topics, _group_config, _consumer_config, _handler, _init_args) do
    GenServer.start_link(__MODULE__, :ok)
  end

  def produce_sync(_client, topic, partition, key, value) do
    send self, [:produce_sync, topic, partition, key, value]
    :ok
  end

  def get_partitions_count(_client, _topic), do: {:ok, @test_partition_count}

  def init(:ok) do
    {:ok, %{}}
  end
end
