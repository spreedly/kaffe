defmodule Kaffe.WorkerSupervisor do
  @moduledoc """
  Supervise the individual workers.
  """

  use Supervisor

  require Logger

  def start_link do
    Supervisor.start_link(__MODULE__, :ok)
  end

  def start_worker_manager(pid) do
    Supervisor.start_child(pid, worker(Kaffe.WorkerManager, []))
  end

  def start_worker(pid, message_handler, partition) do
    Logger.debug "Starting worker for partition: #{partition}"
    Supervisor.start_child(pid,
      worker(Kaffe.Worker, [message_handler, partition], id: partition))
  end

  def init(:ok) do
    Logger.info "event#startup=#{inspect self()}"

    children = [
    ]

    # If anything fails, the state is inconsistent with the state of
    # `Kaffe.Subscriber` and `Kaffe.GroupMember`. We need the failure
    # to cascade all the way up so that they are terminated.
    supervise(children, strategy: :one_for_all, max_restarts: 0, max_seconds: 1)
  end
end