defmodule Kaffe.GroupMemberSupervisor do
  @moduledoc """
  The top-level supervisor for group members and subscribers.
  """

  use Supervisor

  require Logger

  def start_link do
    Supervisor.start_link(__MODULE__, :ok, name: name())
  end

  def start_worker_supervisor(supervisor_pid, subscriber_name) do
    Supervisor.start_child(supervisor_pid, supervisor(Kaffe.WorkerSupervisor, [subscriber_name]))
  end

  def start_group_member(supervisor_pid, subscriber_name, consumer_group,
      worker_manager_pid, topic) do
    Supervisor.start_child(supervisor_pid, worker(Kaffe.GroupMember,
      [subscriber_name, consumer_group, worker_manager_pid, topic],
      id: :"group_member_#{subscriber_name}_#{topic}"))
  end

  def init(:ok) do
    Logger.info "event#starting=#{__MODULE__}"

    children = [
      worker(Kaffe.GroupManager, [])
    ]

    # If we get a failure, we need to reset so the states are all consistent.
    supervise(children, strategy: :one_for_all, max_restarts: 0, max_seconds: 1)
  end

  defp name do
    :"group_member_supervisor_#{subscriber_name()}"
  end

  defp subscriber_name do
    Kaffe.Config.Consumer.configuration.subscriber_name
  end
end
