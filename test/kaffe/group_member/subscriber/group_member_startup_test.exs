defmodule Kaffe.GroupMemberStartupTest do
  @moduledoc """
  First setup a topic as described in the README.
  """

  use ExUnit.Case

  @moduletag :e2e

  defmodule TestSubscriber do
    def subscribe(subscriber_name, _group_coordinator_pid, _worker_pid, _gen_id, topic, partition, _ops) do
      send(:test_case, {:subscribe, subscriber_name, topic, partition})
      {:ok, self()}
    end

    def stop(_pid), do: nil
  end

  setup do
    Application.put_env(:kaffe, :kafka_mod, :brod)
    Application.put_env(:kaffe, :subscriber_mod, TestSubscriber)
  end

  # Start two consumers and verify that they receive different partition assignments
  test "startup" do
    Process.register(self(), :test_case)

    consumer_config = Application.get_env(:kaffe, :consumer)
    Application.put_env(:kaffe, :consumer, Keyword.put(consumer_config, :subscriber_name, "s1"))
    {:ok, _pid} = Kaffe.GroupMemberSupervisor.start_link()

    consumer_config = Application.get_env(:kaffe, :consumer)
    Application.put_env(:kaffe, :consumer, Keyword.put(consumer_config, :subscriber_name, "s2"))
    {:ok, _pid} = Kaffe.GroupMemberSupervisor.start_link()

    :timer.sleep(Kaffe.Config.Consumer.configuration().rebalance_delay_ms + 100)

    assignments =
      Enum.reduce(0..31, %{}, fn partition, map ->
        receive do
          {:subscribe, subscriber_name, _topic, ^partition} ->
            {_get, res} =
              Map.get_and_update(map, subscriber_name, fn
                nil ->
                  {nil, [partition]}

                list ->
                  {list, [partition | list]}
              end)

            res
        end
      end)

    [list1, list2] = Map.values(assignments)

    assert Enum.to_list(0..31) |> Enum.sort() == Enum.sort(list1 ++ list2)
    assert length(list1) == length(list2)
    assert list1 == list1 -- list2
    assert list2 == list2 -- list1
  end
end
