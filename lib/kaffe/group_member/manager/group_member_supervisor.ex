defmodule Kaffe.GroupMemberSupervisor do
  @moduledoc """
  The top-level supervisor for group members and subscribers.
  """

  use Supervisor

  require Logger

  def start_link do
    Supervisor.start_link(__MODULE__, :ok)
  end

  def start_worker_supervisor(supervisor_pid) do
    Supervisor.start_child(supervisor_pid, supervisor(Kaffe.WorkerSupervisor, []))
  end

  def start_group_member(supervisor_pid, subscriber_name, consumer_group,
      worker_manager_pid, topic, configured_offset) do
    Supervisor.start_child(supervisor_pid, worker(Kaffe.GroupMember,
      [subscriber_name, consumer_group, worker_manager_pid, topic, configured_offset], id: topic))
  end

  def init(:ok) do
    Logger.debug "event#starting"

    children = [
      worker(Kaffe.GroupManager, [])
    ]

    # If we get a failure, we need to reset so the states are all consistent.
    supervise(children, strategy: :one_for_all, max_restarts: 0, max_seconds: 1)
  end
end
