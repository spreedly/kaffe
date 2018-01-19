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
      offset: nil
  end

  def start_link() do
    GenServer.start_link(__MODULE__, [self()], name: name())
  end

  # to manually subscribe to new topics (not from config), at any point in time
  def subscribe_topics(topics) do
    GenServer.call(name(), {:subscribe_topics, topics})
  end

  # to manually unsubscribe from existing topics (not from config), at any point in time
  def unsubscribe_topics(topics) do
    GenServer.call(name(), {:unsubscribe_topics, topics})
  end

  # returns the current subscribed topics
  def get_topics() do
    GenServer.call(name(), {:get_topics})
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

    {:noreply, state}
  end

  def handle_call({:unsubscribe_topics, old_topics}, _from, %State{topics: topics} = state) do
    unsubscribed_topics = Enum.flat_map(old_topics, fn(old_topic) ->
      if ! Enum.member?(topics, old_topic) do
        Logger.info "Not unsubscribing from #{old_topic}, already unsubscribed"
        []
      else
        :ok = GroupMemberSupervisor.stop_group_member(state.supervisor_pid, state.subscriber_name, old_topic)
        Logger.debug "Unsubscribed from topic: #{old_topic}"
        [old_topic]
      end
    end)
    {:reply, {:ok, unsubscribed_topics}, %State{state|topics: topics -- unsubscribed_topics } }
  end


  def handle_call({:get_topics}, _from, %State{topics: topics} = state) do
    {:reply, {:ok, topics}, state}
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
