defmodule Kaffe.GroupMemberTest do

  use ExUnit.Case

  alias Kaffe.GroupMember

  defmodule TestKafka do
    def start_consumer(_subscriber_name, _topic, _ops) do
      send :test_case, {:start_consumer}
      :ok
    end
  end

  defmodule TestGroupCoordinator do
    def start_link(_subscriber_name, _consumer_group, _topics, _group_config, _module, _pid) do
      send :test_case, {:group_coordinator_start_link}
      {:ok, self()}
    end
  end

  defmodule TestWorkerManager do
    def worker_for(_pid, _topic, _partition) do
      send :test_case, {:worker_for}
      {:ok, self()}
    end
  end

  defmodule TestSubscriber do
    def subscribe(_subscriber_name, _group_coordinator_pid, _worker_pid, _gen_id, _topic, _partition, _ops) do
      send :test_case, {:subscriber, {:subscribe}}
      {:ok, self()}
    end
    def stop(_subscriber_pid) do
      send :test_case, {:subscriber, {:stop}}
    end
  end

  setup do
    Application.put_env(:kaffe, :kafka_mod, TestKafka)
    Application.put_env(:kaffe, :group_coordinator_mod, TestGroupCoordinator)
    Application.put_env(:kaffe, :worker_manager_mod, TestWorkerManager)
    Application.put_env(:kaffe, :subscriber_mod, TestSubscriber)
  end

  test "handle assignments_received" do

    Process.register(self(), :test_case)
    
    {:ok, pid} = GroupMember.start_link("subscriber_name", "consumer_group", self(), "topic")

    GroupMember.assignments_received(pid, self(), 1, [{:brod_received_assignment, "topic", 0, 1}])

    :timer.sleep Kaffe.Config.Consumer.configuration.rebalance_delay_ms

    assert_receive {:start_consumer}
    assert_receive {:group_coordinator_start_link}
    assert_receive {:worker_for}
    assert_receive {:subscriber, {:subscribe}}
  end

  test "handle assignments_revoked" do

    Process.register(self(), :test_case)
    
    {:ok, pid} = GroupMember.start_link("subscriber_name", "consumer_group", self(), "topic")

    GroupMember.assignments_received(pid, self(), 1, [{:brod_received_assignment, "topic", 0, 1}])

    :timer.sleep Kaffe.Config.Consumer.configuration.rebalance_delay_ms

    GroupMember.assignments_revoked(pid)

    assert_receive {:start_consumer}
    assert_receive {:group_coordinator_start_link}
    assert_receive {:worker_for}
    assert_receive {:subscriber, {:subscribe}}
    assert_receive {:subscriber, {:stop}}, 300
  end

  test "handle assignments_received without assignments_revoked" do

    Process.register(self(), :test_case)
    
    {:ok, pid} = GroupMember.start_link("subscriber_name", "consumer_group", self(), "topic")

    GroupMember.assignments_received(pid, self(), 1, [{:brod_received_assignment, "topic", 0, 1}])

    :timer.sleep Kaffe.Config.Consumer.configuration.rebalance_delay_ms

    GroupMember.assignments_received(pid, self(), 1, [{:brod_received_assignment, "topic", 1, 1}])

    assert_receive {:start_consumer}
    assert_receive {:group_coordinator_start_link}
    assert_receive {:worker_for}
    assert_receive {:subscriber, {:subscribe}}
    assert_receive {:subscriber, {:stop}}, 300
    assert_receive {:worker_for}
    assert_receive {:subscriber, {:subscribe}}
  end

end
