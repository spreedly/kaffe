defmodule Kaffe.WorkerSupervisor do
  @moduledoc """
  Supervise the individual workers.
  """

  use Supervisor

  require Logger

  def start_link(subscriber_name) do
    Supervisor.start_link(__MODULE__, subscriber_name, name: name(subscriber_name))
  end

  def start_worker_manager(pid, subscriber_name) do
    Supervisor.start_child(pid, worker(Kaffe.WorkerManager, [subscriber_name]))
  end

  def start_worker(pid, message_handler, subscriber_name, partition) do
    Logger.debug "Starting worker for partition: #{partition}"
    Supervisor.start_child(pid,
      worker(Kaffe.Worker, [message_handler, subscriber_name, partition],
        id: :"worker_#{subscriber_name}_#{partition}"))
  end

  def init(subscriber_name) do
    Logger.info "event#startup=#{__MODULE__} subscriber_name=#{subscriber_name}"

    children = [
    ]

    # If anything fails, the state is inconsistent with the state of
    # `Kaffe.Subscriber` and `Kaffe.GroupMember`. We need the failure
    # to cascade all the way up so that they are terminated.
    supervise(children, strategy: :one_for_all, max_restarts: 0, max_seconds: 1)
  end

  defp name(subscriber_name) do
    :"worker_supervisor_#{subscriber_name}"
  end

end
