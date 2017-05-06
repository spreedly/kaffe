defmodule Kaffe.WorkerTest do

  use ExUnit.Case

  alias Kaffe.Worker

  defmodule TestSubscriber do
    def ack_messages(_subscriber_pid, topic, partition, generation_id, offset) do
      send :test_case, {:ack_messages, {topic, partition, generation_id, offset}}
    end
  end
  
  defmodule TestHandler do
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

  test "handle messages" do

    {:ok, worker_pid} = Worker.start_link(TestHandler, "subscriber_name", 0)
    Worker.process_messages(worker_pid, self(), "topic", 1, 2,
      [%{key: :one, offset: 100}, %{key: :two, offset: 101}])

    assert_receive {:handle_messages, [%{key: :one, offset: 100}, %{key: :two, offset: 101}]}
    assert_receive {:ack_messages, {"topic", 1, 2, 101}}
  end
end
