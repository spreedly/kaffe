defmodule Kaffe.GroupMemberTest do
  @moduledoc """
  First setup a topic like the one in `script/test-setup.sh`
  """

  use ExUnit.Case
  
  @moduletag :e2e

  defmodule TestSubscriber do
    def subscribe(_subscriber_name, group_coordinator_pid, _worker_pid, _gen_id, topic, partition, _ops) do
      send :test_case, {:subscribe, group_coordinator_pid, topic, partition}
      {:ok, self()}
    end
  end

  setup do
    Application.put_env(:kaffe, :kafka_mod, :brod)
    Application.put_env(:kaffe, :subscriber_mod, TestSubscriber)
  end


  # Start two consumers and verify that they receive different partition assignments
  test "startup" do
    Process.register(self(), :test_case)
    {:ok, _pid} = Kaffe.GroupMemberSupervisor.start_link()
    {:ok, _pid} = Kaffe.GroupMemberSupervisor.start_link()

    :timer.sleep 11_000

    assignments = Enum.reduce(0..31, %{}, fn partition, map ->
      receive do
        {:subscribe, group_coordinator_pid, _topic, ^partition} ->
          {_get, res} = Map.get_and_update(map, group_coordinator_pid, fn
            nil ->
              {nil, [partition]}
            list ->
              {list, [partition | list]}
          end)
          res
      end
    end)

    [list1, list2] = Map.values(assignments)

    assert Enum.to_list(0..31) |> Enum.sort == Enum.sort(list1 ++ list2)
    assert length(list1) == length(list2)
    assert list1 == list1 -- list2
    assert list2 == list2 -- list1

  end

end
