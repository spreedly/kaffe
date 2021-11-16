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
    link = Supervisor.start_link({Kaffe.WorkerManager, [subscriber_name]}, [])
    Supervisor.start_child(pid, link)
  end

  def start_worker(pid, message_handler, subscriber_name, worker_name) do
    Logger.debug("Starting worker: #{worker_name}")

    Supervisor.start_child(
      pid,
      Supervisor.start_link({Kaffe.Worker, [message_handler, subscriber_name, worker_name]},
        id: :"worker_#{subscriber_name}_#{worker_name}"
      )
    )
  end

  def init(subscriber_name) do
    Logger.info("event#startup=#{__MODULE__} subscriber_name=#{subscriber_name}")

    children = []

    # If anything fails, the state is inconsistent with the state of
    # `Kaffe.Subscriber` and `Kaffe.GroupMember`. We need the failure
    # to cascade all the way up so that they are terminated.
    Supervisor.start_link(children, strategy: :one_for_all, max_restarts: 0, max_seconds: 1)
  end

  defp name(subscriber_name) do
    :"#{__MODULE__}.#{subscriber_name}"
  end
end
