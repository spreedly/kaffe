defmodule TestBrod do
  use GenServer

  @test_partition_count Application.get_env(:kaffe, :test_partition_count)

  def start_client(_endpoints, _client_name, _producer_config) do
    GenServer.start_link(__MODULE__, :ok, name: TestBrod)
  end

  def start_link_group_subscriber(_client, _consumer_group, _topics, _group_config, _consumer_config, _handler, _init_args) do
    GenServer.start_link(__MODULE__, :ok)
  end

  def produce_sync(_client, topic, partition, key, value) do
    GenServer.call(TestBrod, {:produce_sync, topic, partition, key, value})
  end

  def get_partitions_count(_client, _topic), do: {:ok, @test_partition_count}

  def set_produce_response(response) do
    GenServer.call(TestBrod, {:set_produce_response, response})
  end

  def init(:ok) do
    {:ok, %{produce_response: :ok}}
  end

  def handle_call({:produce_sync, topic, partition, key, value}, _from, state) do
    send :test_case, [:produce_sync, topic, partition, key, value]
    {:reply, state.produce_response, state}
  end
  def handle_call({:set_produce_response, response}, _from, state) do
    {:reply, response, %{state | produce_response: response}}
  end
end
