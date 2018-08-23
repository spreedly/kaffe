defmodule Kaffe.GroupMemberSupervisor do
  @moduledoc """
  The top-level supervisor for consumer group members.

  Consuming messages from a Kafka topic as part of a consumer group is broken
  out into the following distinct components:

    - GroupMember
    - Subscriber
    - Worker
    - WorkerManager
    - WorkerSupervisor

  The GroupMember implements the :brod_group_member behaviour and is responsible
  for receiving assignments from Kafka and managing how subscribers and workers
  are configured and allocated. All message processing is then delegated to the
  created Subscribers and Workers.

  Subscribers subscribe to Kafka topics and are responsble for receiving lists
  of messages, passing messages to workers for processing, and committing
  offsets after processing.

  Workers wrap application message handlers and notify subscribers about
  whether or not commit offsets back to Kafka.
  """

  use Supervisor
  require Logger

  def start_link do
    Supervisor.start_link(__MODULE__, :ok, name: name())
  end

  def start_worker_supervisor(supervisor_pid, subscriber_name) do
    Supervisor.start_child(supervisor_pid, supervisor(Kaffe.WorkerSupervisor, [subscriber_name]))
  end

  def start_group_member(
        supervisor_pid,
        subscriber_name,
        consumer_group,
        worker_manager_pid,
        topic
      ) do
    Supervisor.start_child(
      supervisor_pid,
      worker(
        Kaffe.GroupMember,
        [subscriber_name, consumer_group, worker_manager_pid, topic],
        id: :"group_member_#{subscriber_name}_#{topic}"
      )
    )
  end

  def init(:ok) do
    Logger.info("event#starting=#{__MODULE__}")

    children = [
      worker(Kaffe.GroupManager, [])
    ]

    # If we get a failure, we need to reset so the states are all consistent.
    supervise(children, strategy: :one_for_all, max_restarts: 0, max_seconds: 1)
  end

  defp name do
    :"#{__MODULE__}.#{subscriber_name()}"
  end

  defp subscriber_name do
    Kaffe.Config.Consumer.configuration().subscriber_name
  end
end
