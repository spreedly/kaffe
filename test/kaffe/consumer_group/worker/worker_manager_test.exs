defmodule Kaffe.WorkerManagerTest do
  use ExUnit.Case

  alias Kaffe.WorkerManager

  defmodule TestWorkerSupervisor do
    def start_worker(_, _, _, worker_name) do
      case worker_name do
        :worker_0 -> {:ok, 1}
        :worker_topic1_0 -> {:ok, 1}
        :worker_topic2_0 -> {:ok, 2}
      end
    end
  end

  setup do
    Application.put_env(:kaffe, :worker_supervisor_mod, TestWorkerSupervisor)
    {:ok, worker_manager_pid} = WorkerManager.start_link(["worker_test", consumer_config()])
    %{worker_manager_pid: worker_manager_pid}
  end

  test "allocate worker per partition", %{worker_manager_pid: worker_manager_pid} do
    worker_pid1 = WorkerManager.worker_for(worker_manager_pid, "topic1", 0)
    worker_pid2 = WorkerManager.worker_for(worker_manager_pid, "topic2", 0)

    assert worker_pid1 == worker_pid2
  end

  test "allocate worker per topic partition", %{worker_manager_pid: worker_manager_pid} do
    configure_strategy(worker_manager_pid, :worker_per_topic_partition)

    worker_pid1 = WorkerManager.worker_for(worker_manager_pid, "topic1", 0)
    worker_pid2 = WorkerManager.worker_for(worker_manager_pid, "topic2", 0)

    refute worker_pid1 == worker_pid2
  end

  defp consumer_config() do
    Kaffe.Config.Consumer.configuration("subscriber_name")
  end

  defp configure_strategy(worker_manager_pid, strategy) do
    :sys.replace_state(worker_manager_pid, fn state ->
      new_config = %{state.config | worker_allocation_strategy: strategy}
      %{state | config: new_config}
    end)
  end
end
