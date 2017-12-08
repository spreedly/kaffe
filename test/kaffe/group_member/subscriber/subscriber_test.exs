defmodule Kaffe.SubscriberTest do

  use ExUnit.Case

  require Kaffe.Subscriber
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

  defmodule TestWorker do
    def process_messages(_pid, _subscriber_pid, _topic, _partition, _generation_id, _messages) do
      send :test_case, {:process_messages}
    end
  end

  setup do
    Application.put_env(:kaffe, :kafka_mod, TestKafka)
    Application.put_env(:kaffe, :worker_mod, TestWorker)
  end

  test "handle message set" do

    Process.register(self(), :test_case)
    {:ok, kafka_pid} = TestKafka.start_link(0)

    {:ok, pid} = Subscriber.subscribe("subscriber_name", self(), self(), 1, "topic", 0, [])
    send(pid, {self(), build_message_set()})

    assert_receive {:subscribe, {:ok, ^kafka_pid}}
    assert_receive {:process_messages}
  end

  test "handle kafka_fetch_error" do

    Process.register(self(), :test_case)
    {:ok, kafka_pid} = TestKafka.start_link(0)

    {:ok, pid} = Subscriber.subscribe("subscriber_name", self(), self(), 1, "topic", 0, [])
    Process.unlink(pid)
    Process.monitor(pid)
    send(pid, {self(), {:kafka_fetch_error, "topic", 1,
      :NotLeaderForPartition, "This server is not the leader for that topic-partition."}})

    assert_receive {:subscribe, {:ok, ^kafka_pid}}
    refute_receive {:process_messages}
    assert_receive {:DOWN, _ref, :process, ^pid,
      {:shutdown, {:kafka_fetch_error, "topic", 1, :NotLeaderForPartition, _reason}}}
  end

  test "handle consumer down" do

    Process.register(self(), :test_case)
    {:ok, kafka_pid} = TestKafka.start_link(0)

    {:ok, pid} = Subscriber.subscribe("subscriber_name", self(), self(), 1, "topic", 0, [])
    Process.unlink(pid)
    Process.monitor(pid)
    send(pid, {:DOWN, make_ref(), :process, kafka_pid, :failure_message})

    assert_receive {:subscribe, {:ok, ^kafka_pid}}
    refute_receive {:process_messages}
    assert_receive {:DOWN, _ref, :process, ^pid, {:shutdown, {:consumer_down, :failure_message}}}
  end

  test "handle recovered subscribe failure" do

    Process.register(self(), :test_case)
    {:ok, kafka_pid} = TestKafka.start_link(1)

    Subscriber.subscribe("subscriber_name", self(), self(), 1, "topic", 0, [])

    assert_receive {:subscribe, {:error, :no_available_offsets}}
    assert_receive {:subscribe, {:ok, ^kafka_pid}}
  end

  test "handle complete subscribe failure" do

    Process.register(self(), :test_case)

    Process.flag(:trap_exit, true)

    {:ok, kafka_pid} = TestKafka.start_link(2)
    Process.monitor(kafka_pid)

    {:ok, subscriber_pid} = Subscriber.subscribe("subscriber_name", self(), self(), 1, "topic", 0, [])

    assert_receive {:subscribe, {:error, :no_available_offsets}}
    assert_receive {:subscribe, {:error, :no_available_offsets}}
    assert_receive {:EXIT, ^subscriber_pid, _reason}
  end

  defp build_message_set do
    {:kafka_message_set, "topic", 0, 1, build_message_list()}
  end

  defp build_message_list do
    Enum.map(1..10, fn (n) ->
      Subscriber.kafka_message(
        offset: n,
        magic_byte: 0,
        attributes: 0,
        key: "key-#{n}",
        value: "#{n}",
        crc: -1
      )
    end)
  end
end
