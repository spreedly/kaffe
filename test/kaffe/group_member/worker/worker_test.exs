defmodule Kaffe.WorkerTest do

  use ExUnit.Case

  alias Kaffe.Worker

  defmodule TestSubscriber do
    def commit_offsets(_subscriber_pid, topic, partition, generation_id, offset) do
      send :test_case, {:commit_offsets, {topic, partition, generation_id, offset}}
    end

    def request_more_messages(_subscriber_pid, offset) do
      send :test_case, {:request_more_messages, {offset}}
    end
  end

  defmodule TestHandler do
    def handle_messages([%{key: :one, offset: 100}, %{key: :two, offset: 888}] = messages) do
      send :test_case, {:handle_messages, messages}
      {:ok, :no_commit}
    end
    def handle_messages([%{key: :one, offset: 100}, %{key: :two, offset: 999}] = messages) do
      send :test_case, {:handle_messages, messages}
      {:ok, 100}
    end
    def handle_messages(messages) do
      send :test_case, {:handle_messages, messages}
      :ok
    end
  end

  setup do
    Application.put_env(:kaffe, :subscriber_mod, TestSubscriber)
    Process.register(self(), :test_case)
    :ok
  end

  test "handle messages and commit back offset" do
    {:ok, worker_pid} = Worker.start_link(TestHandler, "subscriber_name", 0)

    Worker.process_messages(worker_pid, self(), "topic", 1, 2,
      [%{key: :one, offset: 100}, %{key: :two, offset: 101}])

    assert_receive {:handle_messages, [%{key: :one, offset: 100}, %{key: :two, offset: 101}]}
    assert_receive {:commit_offsets, {"topic", 1, 2, 101}}
    assert_receive {:request_more_messages, {101}}
  end

  test "handle messages and maintain offset" do
    {:ok, worker_pid} = Worker.start_link(TestHandler, "subscriber_name", 0)

    Worker.process_messages(worker_pid, self(), "topic", 1, 2,
      [%{key: :one, offset: 100}, %{key: :two, offset: 888}])

    assert_receive {:handle_messages,
      [%{key: :one, offset: 100}, %{key: :two, offset: 888}]}
    refute_received {:commit_offsets, {"topic", 1, 2, 888}}
    assert_receive {:request_more_messages, {888}}
  end

  test "handle messages and commit back specific offset" do
    {:ok, worker_pid} = Worker.start_link(TestHandler, "subscriber_name", 0)

    Worker.process_messages(worker_pid, self(), "topic", 1, 2,
      [%{key: :one, offset: 100}, %{key: :two, offset: 999}])

    assert_receive {:handle_messages,
      [%{key: :one, offset: 100}, %{key: :two, offset: 999}]}
    assert_receive {:commit_offsets, {"topic", 1, 2, 100}}
    assert_receive {:request_more_messages, {100}}
  end
end
