defmodule Kaffe.GroupManager do
  @moduledoc """
  This is the main process for bootstrapping the full supervision tree to
  consume a Kafka topic via a subscriber/worker combo per topic per partition
  as part of a consumer group.

  See Kaffe.GroupMemberSupervisor for distinct components.

  The process begins by starting the client connection to Kafka. Then group
  members are created for each of the configured topics.

  Note that this module is is not involved in any message processing, rather
  it's role is to ensure that all of the relevant services are running.
  """

  use GenServer
  use Retry
  require Logger

  defmodule State do
    @moduledoc """
    The running state of the consumer group manager.
    """
    defstruct supervisor_pid: nil,
              subscriber_name: nil,
              consumer_group: nil,
              topics: nil,
              offset: nil,
              worker_manager_pid: nil
  end

  defmodule ClientDownException do
    defexception [:message]

    def exception(_term) do
      %ClientDownException{message: "Kafka client is down"}
    end
  end

  ## ==========================================================================
  ## Public API
  ## ==========================================================================
  def start_link() do
    GenServer.start_link(__MODULE__, [self()], name: name())
  end

  @doc """
  Dynamically subscribe to topics in addition to the configured topics.
  Returns the newly subscribed topics. This may not include all values if any are already subscribed to.
  """
  def subscribe_to_topics(topics) do
    GenServer.call(name(), {:subscribe_to_topics, topics})
  end

  @doc """
  List of currently subscribed topics.
  """
  def list_subscribed_topics do
    GenServer.call(name(), {:list_subscribed_topics})
  end

  ## ==========================================================================
  ## Callbacks
  ## ==========================================================================
  def init([supervisor_pid]) do
    Logger.info("event#startup=#{__MODULE__} name=#{name()}")

    config = Kaffe.Config.Consumer.configuration()

    case kafka().start_client(config.endpoints, config.subscriber_name, config.consumer_config) do
      :ok ->
        :ok

      {_, :already_present} ->
        Logger.info("The brod client is already present, continuing.")
    end

    GenServer.cast(self(), {:start_group_members})

    {:ok,
     %State{
       supervisor_pid: supervisor_pid,
       subscriber_name: config.subscriber_name,
       consumer_group: config.consumer_group,
       topics: config.topics
     }}
  end

  @doc """
  Start the subscribers and workers to process message sets

  Worker are booted before the subscribers so when the subscribers receive the
  first messages, we know there will be a worker to do the actual processing work
  """
  def handle_cast({:start_group_members}, state) do
    Logger.debug("Starting worker supervisors for group manager: #{inspect(self())}")

    {:ok, worker_supervisor_pid} =
      group_member_supervisor().start_worker_supervisor(state.supervisor_pid, state.subscriber_name)

    {:ok, worker_manager_pid} = worker_supervisor().start_worker_manager(worker_supervisor_pid, state.subscriber_name)

    state = %State{state | worker_manager_pid: worker_manager_pid}

    :ok = subscribe_to_topics(state, state.topics)

    {:noreply, state}
  end

  @doc """
  Subscribe to a new set of topics. The new list of subscribed topics will only include
  the requested topics and none of the currently configured topics.
  """
  def handle_call({:subscribe_to_topics, requested_topics}, _from, %State{topics: topics} = state) do
    new_topics = requested_topics -- topics
    :ok = subscribe_to_topics(state, new_topics)

    {:reply, {:ok, new_topics}, %State{state | topics: state.topics ++ new_topics}}
  end

  @doc """
  List the currently subscribed topics
  """
  def handle_call({:list_subscribed_topics}, _from, %State{topics: topics} = state) do
    {:reply, topics, state}
  end

  ## ==========================================================================
  ## Helpers
  ## ==========================================================================

  defp subscribe_to_topics(state, topics) do
    Logger.debug("Starting group members for the following topics: #{inspect(topics)}")

    retry with: exponential_backoff() |> expiry(client_down_retry_expire()),
          rescue_only: [Kaffe.GroupManager.ClientDownException] do
      Enum.each(topics, fn topic ->
        case subscribe_to_topic(state, topic) do
          {:ok, _pid} ->
            Logger.debug("Started group member for topic: #{topic}")
            :ok

          error ->
            Logger.debug("Starting group member for #{topic} failed, attempting retry with exponential backoff")

            is_client_down_error?(error)
            |> do_a_retry?(error)
        end
      end)
    after
      :ok ->
        Logger.debug("Group members succesfully started")
    else
      {:error, reason} = error ->
        Logger.error("Starting group members failed: #{inspect(reason)}")
        error

      _ = error ->
        Logger.error("Starting group members failed: #{inspect(error)}")
        {:error, error}
    end
  end

  defp do_a_retry?(true, _error), do: raise(Kaffe.GroupManager.ClientDownException)
  defp do_a_retry?(false, error), do: raise(error)

  defp subscribe_to_topic(state, topic) do
    group_member_supervisor().start_group_member(
      state.supervisor_pid,
      state.subscriber_name,
      state.consumer_group,
      state.worker_manager_pid,
      topic
    )
  end

  defp kafka do
    Application.get_env(:kaffe, :kafka_mod, :brod)
  end

  defp name do
    :"#{__MODULE__}.#{subscriber_name()}"
  end

  defp subscriber_name do
    Kaffe.Config.Consumer.configuration().subscriber_name
  end

  defp group_member_supervisor do
    Application.get_env(:kaffe, :group_member_supervisor_mod, Kaffe.GroupMemberSupervisor)
  end

  defp worker_supervisor do
    Application.get_env(:kaffe, :worker_supervisor_mod, Kaffe.WorkerSupervisor)
  end

  defp client_down_retry_expire() do
    Kaffe.Config.Consumer.configuration().client_down_retry_expire
  end

  # Brod client errors are erlang exceptions and are hard to pattern match correctly.
  # This function casts the error to a string, and returns if the error is a client down error
  defp is_client_down_error?({:error, error}) do
    error_string = "#{inspect(error)}"

    cond do
      String.match?(error_string, ~r(:econnrefused)) ->
        true

      String.match?(error_string, ~r({:error, :client_down})) ->
        true

      true ->
        false
    end
  end
end
