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

  def start_link(config_key) do
    Kaffe.Config.Consumer.validate_configuration!()
    config = Kaffe.Config.Consumer.configuration(config_key)
    Supervisor.start_link(__MODULE__, config, name: name(config))
  end

  def start_worker_supervisor(supervisor_pid, subscriber_name) do
    Supervisor.start_child(
      supervisor_pid,
      %{
        id: :"Kaffe.WorkerSupervisor.#{subscriber_name}",
        start: {Kaffe.WorkerSupervisor, :start_link, [subscriber_name]},
        type: :supervisor
      }
    )
  end

  def start_group_member(
        supervisor_pid,
        subscriber_name,
        consumer_group,
        worker_manager_pid,
        topic,
        config
      ) do
    Supervisor.start_child(
      supervisor_pid,
      %{
        id: :"group_member_#{subscriber_name}_#{topic}",
        start: {
          Kaffe.GroupMember,
          :start_link,
          [subscriber_name, consumer_group, worker_manager_pid, topic, config]
        }
      }
    )
  end

  def init(config) do
    Logger.info("event#starting=#{__MODULE__}")

    children = [
      %{
        id: Kaffe.GroupManager,
        start: {Kaffe.GroupManager, :start_link, [config]}
      }
    ]

    # If we get a failure, we need to reset so the states are all consistent.
    Supervisor.init(children, strategy: :one_for_all, max_restarts: 0, max_seconds: 1)
  end

  defp name(config) do
    :"#{__MODULE__}.#{config.subscriber_name}"
  end
end
