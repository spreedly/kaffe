defmodule Kaffe.SubscriberTest do

  use ExUnit.Case

  alias Kaffe.Subscriber

  defmodule TestKafka do
    use GenServer
    def start_link(failures) do
      GenServer.start_link(__MODULE__, failures, name: __MODULE__)
    end
    def init(failures) do
      {:ok, %{failures: failures}}
    end
    def subscribe(_subscriber_name, _subscriber_pid, _topic, _partition, _ops) do
      GenServer.call(__MODULE__, {:subscribe})
    end
    def handle_call({:subscribe}, _from, %{failures: failures} = state) when failures > 0 do
      send :test_case, {:subscribe, {:error, :no_available_offsets}}
      {:reply, {:error, :no_available_offsets}, %{state | failures: (failures - 1)}}
    end
    def handle_call({:subscribe}, _from, state) do
      send :test_case, {:subscribe, {:ok, self()}}
      {:reply, {:ok, self()}, state}
    end
  end

  setup do
    Application.put_env(:kaffe, :kafka_mod, TestKafka)
  end

  test "handle recovered subscribe failure" do

    Process.register(self(), :test_case)
    {:ok, kafka_pid} = TestKafka.start_link(1)

    Subscriber.subscribe("subscriber_name", self(), self(), 1, "topic", 0, [])

    :timer.sleep 100

    assert_received {:subscribe, {:error, :no_available_offsets}}
    assert_received {:subscribe, {:ok, ^kafka_pid}}
  end

  test "handle complete subscribe failure" do

    Process.register(self(), :test_case)

    Process.flag(:trap_exit, true)

    {:ok, kafka_pid} = TestKafka.start_link(2)
    Process.monitor(kafka_pid)

    {:ok, subscriber_pid} = Subscriber.subscribe("subscriber_name", self(), self(), 1, "topic", 0, [])

    :timer.sleep 100

    assert_received {:subscribe, {:error, :no_available_offsets}}
    assert_received {:subscribe, {:error, :no_available_offsets}}
    assert_received {:EXIT, ^subscriber_pid, _reason}
  end
end