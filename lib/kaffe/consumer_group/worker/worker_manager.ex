defmodule Kaffe.WorkerManager do
  @moduledoc """
  Manage topic/partition-to-worker assignments. Subscribers get workers
  from here.

  This process manages the workers, while delegating to
  `Kaffe.WorkerSupervisor` to start each worker under supervision.

  Workers are allocated based on a configured strategy
  (`worker_allocation_strategy` in the `consumer` config): either one
  worker per topic/partition, or one worker for a partition across
  topics.

  The first strategy is useful for higher throughput and increased
  flexibility. The second is useful when mutliple input topics may have
  the same messages (identified by key) and those messages must be
  processsed sequentially.

  The table of workers is stored in an ETS table, `:kaffe_workers`.
  """

  use GenServer

  require Logger

  ## ==========================================================================
  ## Public API
  ## ==========================================================================
  def start_link([subscriber_name, config]) do
    GenServer.start_link(__MODULE__, [self(), subscriber_name, config], name: name(subscriber_name))
  end

  def worker_for(pid, topic, partition) do
    GenServer.call(pid, {:worker_for, topic, partition})
  end

  ## ==========================================================================
  ## Callbacks
  ## ==========================================================================
  def init([supervisor_pid, subscriber_name, config]) do
    Logger.info("event#starting=#{__MODULE__} subscriber_name=#{subscriber_name} supervisor=#{inspect(supervisor_pid)}")

    worker_table = :ets.new(:kaffe_workers, [:set, :protected])

    {:ok,
     %{
       supervisor_pid: supervisor_pid,
       subscriber_name: subscriber_name,
       worker_table: worker_table,
       config: config
     }}
  end

  def handle_call({:worker_for, topic, partition}, _from, state) do
    Logger.debug("Allocating worker: #{topic} / #{partition}")
    worker_pid = allocate_worker(topic, partition, state)
    {:reply, worker_pid, state}
  end

  ## ==========================================================================
  ## Helpers
  ## ==========================================================================
  defp allocate_worker(topic, partition, %{worker_table: worker_table} = state) do
    worker_name = worker_name(topic, partition, state.config.worker_allocation_strategy)

    case :ets.lookup(worker_table, worker_name) do
      [{^worker_name, worker_pid}] -> worker_pid
      [] -> start_worker(worker_name, state)
    end
  end

  defp start_worker(worker_name, state) do
    config = state.config
    Logger.debug("Creating worker: #{worker_name}")

    worker_supervisor().start_worker(
      state.supervisor_pid,
      config.message_handler,
      state.subscriber_name,
      worker_name
    )
    |> capture_worker(worker_name, state)
  end

  defp capture_worker({:ok, pid}, worker_name, %{worker_table: worker_table}) do
    true = :ets.insert(worker_table, {worker_name, pid})
    pid
  end

  def worker_name(topic, partition, worker_allocation_strategy) do
    case worker_allocation_strategy do
      :worker_per_partition -> :"worker_#{partition}"
      :worker_per_topic_partition -> :"worker_#{topic}_#{partition}"
    end
  end

  defp worker_supervisor do
    Application.get_env(:kaffe, :worker_supervisor_mod, Kaffe.WorkerSupervisor)
  end

  defp name(subscriber_name) do
    :"#{__MODULE__}.#{subscriber_name}"
  end
end
