defmodule Kaffe.GroupManager do
  @moduledoc """
  This is the main process for launching group members and workers.

  The process begins by starting the client connection to Kafka. Then, group
  members are created for each of the configured topics.

  This is also the place where subscribers get the PID for a Worker.
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
    GenServer.start_link(__MODULE__, [self()])  
  end

  ## Callbacks

  def init([supervisor_pid]) do
    Logger.info "event#startup=#{inspect self()}"

    config = Kaffe.Config.Consumer.configuration

    :ok = kafka().start_client(config.endpoints, config.subscriber_name, config.consumer_config)

    GenServer.cast(self(), {:start_group_members})

    {:ok, %State{supervisor_pid: supervisor_pid,
                subscriber_name: config.subscriber_name,
                consumer_group: config.consumer_group,
                topics: config.topics,
                offset: Kaffe.Config.Consumer.begin_offset}}
  end

  def handle_cast({:start_group_members}, state) do

    Logger.debug "Starting worker supervisors for group manager: #{inspect self()}"
    
    {:ok, worker_supervisor_pid} = GroupMemberSupervisor.start_worker_supervisor(state.supervisor_pid)
    {:ok, worker_manager_pid} = WorkerSupervisor.start_worker_manager(worker_supervisor_pid)

    Enum.each(state.topics, fn(topic) ->
      Logger.debug "Starting group member for topic: #{topic}"
      {:ok, _pid} = GroupMemberSupervisor.start_group_member(
        state.supervisor_pid,
        state.subscriber_name,
        state.consumer_group,
        worker_manager_pid,
        topic,
        state.offset)
    end)
    {:noreply, state}
  end

  defp kafka do
    Application.get_env(:kaffe, :kafka_mod, :brod)
  end

end