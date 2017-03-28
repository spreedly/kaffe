defmodule Kaffe.WorkerTest do

  use ExUnit.Case

  alias Kaffe.Worker
  
  defmodule TestHandler do
    def handle_messages(messages) do
      send :test_case, {:handle_messages, messages}
      :ok
    end
  end

  test "use list handler" do
    Process.register(self(), :test_case)
    {:ok, worker_pid} = Worker.start_link(TestHandler, 0)
    Worker.process_messages(worker_pid, [%{message: :one}, %{message: :two}])

    :timer.sleep 100

    assert_received {:handle_messages, [%{message: :one}, %{message: :two}]}
  end
end