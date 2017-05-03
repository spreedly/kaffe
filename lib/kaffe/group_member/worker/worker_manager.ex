defmodule Kaffe.WorkerManager do
  @moduledoc """
  Manage partition-to-worker assignments. Subscribers get workers from here.

  This process manages the workers, while delegating to
  `Kaffe.WorkerSupervisor` to start each worker under supervision.

  The table of workers is stored in an ETS table, `:workers`.
  """
  
  use GenServer

  alias Kaffe.WorkerSupervisor

  require Logger

  def start_link(subscriber_name) do
    GenServer.start_link(__MODULE__, [self(), subscriber_name], name: name(subscriber_name))
  end

  def worker_for(pid, partition) do
    GenServer.call(pid, {:worker_for, partition})
  end

  ## Callbacks

  def init([supervisor_pid, subscriber_name]) do
    Logger.info "event#starting=#{__MODULE__} subscriber_name=#{subscriber_name} supervisor=#{inspect supervisor_pid}"

    worker_table = :ets.new(:workers, [:set, :protected])

    {:ok, %{supervisor_pid: supervisor_pid, subscriber_name: subscriber_name, worker_table: worker_table}}
  end
  
  def handle_call({:worker_for, partition}, _from, state) do
    worker_pid = allocate_worker(partition, state)
    {:reply, worker_pid, state}
  end

  ## Private

  defp allocate_worker(partition, %{worker_table: worker_table} = state) do
    case :ets.lookup(worker_table, partition) do
      [{^partition, worker_pid}] -> worker_pid
      [] -> start_worker(partition, state)
    end
  end

  defp start_worker(partition, state) do
    config = Kaffe.Config.Consumer.configuration
    Logger.debug "Creating worker for partition: #{partition}"
    WorkerSupervisor.start_worker(state.supervisor_pid, config.message_handler,
      state.subscriber_name, partition)
    |> capture_worker(partition, state)
  end

  defp capture_worker({:ok, pid}, partition, %{worker_table: worker_table}) do
    Logger.debug "Captured new worker: #{inspect pid} for partition: #{partition}"
    true = :ets.insert(worker_table, {partition, pid})
    pid
  end

  defp name(subscriber_name) do
    :"worker_manager_#{subscriber_name}"
  end

end
