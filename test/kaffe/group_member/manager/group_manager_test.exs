defmodule Kaffe.GroupManagerTest do
  use ExUnit.Case

  alias Kaffe.GroupManager

  defmodule TestKafka do
    def start_client(_endpoints, _subscriber_name, _consumer_config) do
      send :test_case, {:start_client}
      :ok
    end
  end

  defmodule TestGroupMemberSupervisor do
    def start_worker_supervisor(_supervisor_pid, _subscriber_name) do
      send :test_case, {:start_worker_supervisor}
      {:ok, self()}
    end
    def start_group_member(_supervisor_pid, _subscriber_name, _consumer_group,
      _worker_manager_pid, topic) do
      send :test_case, {:start_group_member, topic}
      {:ok, self()}
    end
  end

  defmodule TestWorkerSupervisor do
    def start_worker_manager(_pid, _subscriber_name) do
      send :test_case, {:start_worker_manager}
      {:ok, self()}
    end
  end

  setup do
    Application.put_env(:kaffe, :kafka_mod, TestKafka)
    Application.put_env(:kaffe, :group_member_supervisor_mod, TestGroupMemberSupervisor)
    Application.put_env(:kaffe, :worker_supervisor_mod, TestWorkerSupervisor)
  end

  test "subscribe from config" do

    Process.register(self(), :test_case)

    {:ok, _group_manager_pid} = GroupManager.start_link()

    :timer.sleep Kaffe.Config.Consumer.configuration.rebalance_delay_ms

    assert_receive {:start_client}
    assert_receive {:start_worker_supervisor}
    assert_receive {:start_worker_manager}
    assert_receive {:start_group_member, "kaffe-test"}

    assert ["kaffe-test"] == GroupManager.list_subscribed_topics()

  end

  test "subscribe to topics dynamically" do

    Process.register(self(), :test_case)

    {:ok, _group_manager_pid} = GroupManager.start_link()

    :timer.sleep Kaffe.Config.Consumer.configuration.rebalance_delay_ms

    # startup
    assert_receive {:start_client}
    assert_receive {:start_worker_supervisor}
    assert_receive {:start_worker_manager}
    assert_receive {:start_group_member, "kaffe-test"}

    # subscribe to new topics
    GroupManager.subscribe_to_topics(["so-interesting", "such-random"])

    assert_receive {:start_group_member, "so-interesting"}
    assert_receive {:start_group_member, "such-random"}

    assert [] == (GroupManager.list_subscribed_topics() -- ["kaffe-test", "so-interesting", "such-random"])

  end

  test "duplicate topic subscription does nothing" do

    Process.register(self(), :test_case)

    {:ok, _group_manager_pid} = GroupManager.start_link()

    :timer.sleep Kaffe.Config.Consumer.configuration.rebalance_delay_ms

    # startup
    assert_receive {:start_client}
    assert_receive {:start_worker_supervisor}
    assert_receive {:start_worker_manager}
    assert_receive {:start_group_member, "kaffe-test"}

    # subscribe to new topics
    GroupManager.subscribe_to_topics(["so-interesting", "such-random"])

    assert_receive {:start_group_member, "so-interesting"}
    assert_receive {:start_group_member, "such-random"}

    GroupManager.subscribe_to_topics(["so-interesting"])

    refute_receive {:start_group_member, "so-interesting"}

    assert [] == (GroupManager.list_subscribed_topics() -- ["kaffe-test", "so-interesting", "such-random"])

  end
end
