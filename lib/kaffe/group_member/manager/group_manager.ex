defmodule Kaffe.GroupManager do
  @moduledoc """
  This is the main process for launching group members and workers.

  The process begins by starting the client connection to Kafka. Then, group
  members are created for each of the configured topics.
  """
  
  use GenServer

  alias Kaffe.GroupMemberSupervisor
  alias Kaffe.WorkerSupervisor

  require Logger

  defmodule State do
    defstruct supervisor_pid: nil,
      subscriber_name: nil,
      consumer_group: nil,
      topics: nil,
      offset: nil,
      worker_manager_pid: nil
  end

  def start_link() do
    GenServer.start_link(__MODULE__, [self()], name: name())
  end

  # to manually subscribe to new topics (not from config), at any point in time
  def subscribe_topics(topics) do
    GenServer.call(name(), {:subscribe_topics, topics})
  end

  ## Callbacks

  def init([supervisor_pid]) do
    Logger.info "event#startup=#{__MODULE__} name=#{name()}"

    config = Kaffe.Config.Consumer.configuration

    :ok = kafka().start_client(config.endpoints, config.subscriber_name, config.consumer_config)

    GenServer.cast(self(), {:start_group_members})

    {:ok, %State{supervisor_pid: supervisor_pid,
                subscriber_name: config.subscriber_name,
                consumer_group: config.consumer_group,
                topics: config.topics}}
  end

  def handle_cast({:start_group_members}, state) do

    Logger.debug "Starting worker supervisors for group manager: #{inspect self()}"
    
    {:ok, worker_supervisor_pid} = GroupMemberSupervisor.start_worker_supervisor(
      state.supervisor_pid, state.subscriber_name)
    {:ok, worker_manager_pid} = WorkerSupervisor.start_worker_manager(
      worker_supervisor_pid, state.subscriber_name)

    Enum.each(state.topics, fn(topic) ->
      Logger.debug "Starting group member for topic: #{topic}"
      {:ok, _pid} = GroupMemberSupervisor.start_group_member(
        state.supervisor_pid,
        state.subscriber_name,
        state.consumer_group,
        worker_manager_pid,
        topic)
    end)

    {:noreply, %State{state|worker_manager_pid: worker_manager_pid} }
  end

  def handle_call({:subscribe_topics, new_topics}, _from, %State{topics: topics} = state) do
    subscribed_topics = Enum.flat_map(new_topics, fn(new_topic) ->
      if Enum.member?(topics, new_topic) do
        Logger.info "Not subscribing to #{new_topic}, already subscribed"
        []
      else
        Logger.debug "Starting group member for topic: #{new_topic}"
        {:ok, _pid} = GroupMemberSupervisor.start_group_member(
          state.supervisor_pid,
          state.subscriber_name,
          state.consumer_group,
          state.worker_manager_pid,
          new_topic)
        [new_topic]
      end
    end)
    {:reply, {:ok, subscribed_topics}, %State{state|topics: subscribed_topics ++ topics} }
  end

  defp kafka do
    Application.get_env(:kaffe, :kafka_mod, :brod)
  end

  defp name do
    :"kaffe_group_manager_#{subscriber_name()}"
  end

  defp subscriber_name do
    Kaffe.Config.Consumer.configuration.subscriber_name
  end

end
