defmodule Kaffe.GroupMember do
  @moduledoc """
  Note: The `brod_group_member` behavior is used.

  Consume messages from a Kafka topic for a consumer group. There is one brod
  group member per topic! So as new topics are added to configuration so are
  the number of brod group members. Likewise, if you're using something like
  Heroku Kafka and have multiple dynos for your consumer, there will be a
  Kaffe.GroupMember per dyno and each group member will receive a equal set
  of partition assignments for each topic.

  The actual consumption is delegated to a series of subscribers, see
  `Kaffe.Subscriber` for details on how messages are processed.

  The subscribers are assigned generations. Each generation represents a
  specific configuration. In order to allow the partitions to be rebalanced on
  startup, there is a delay between receiving a set of assignments associated
  with that generation and actually creating the subscribers. If a new
  generation is received in the mean time, the older generation is discarded.

  See the follow for more details:
  https://github.com/klarna/brod/blob/master/src/brod_group_member.erl
  https://github.com/klarna/brucke/blob/master/src/brucke_member.erl
  """

  use GenServer
  require Logger

  @behaviour :brod_group_member

  defmodule State do
    defstruct subscribers: [],
              subscriber_name: nil,
              group_coordinator_pid: nil,
              consumer_group: nil,
              worker_manager_pid: nil,
              topic: nil,
              configured_offset: nil,
              current_gen_id: nil
  end

  ## ==========================================================================
  ## Public API
  ## ==========================================================================
  def start_link(subscriber_name, consumer_group, worker_manager_pid, topic) do
    GenServer.start_link(
      __MODULE__,
      [
        subscriber_name,
        consumer_group,
        worker_manager_pid,
        topic
      ],
      name: name(subscriber_name, topic)
    )
  end

  # Should not receive this
  def get_committed_offsets(_group_member_pid, _topic_partitions) do
    Logger.warn("event#get_committed_offsets")
  end

  # Should not receive this
  def assign_partitions(_pid, _members, _topic_partitions) do
    Logger.warn("event#assign_partitions")
  end

  def assignments_received(pid, _member_id, generation_id, assignments) do
    GenServer.cast(pid, {:assignments_received, generation_id, assignments})
  end

  def assignments_revoked(pid) do
    GenServer.cast(pid, {:assignments_revoked})
  end

  def stop_subscribers_and_workers(subscriber_name, topic) do
    case Process.whereis(name(subscriber_name, topic)) do
      nil ->
        {:error, :not_found}

      pid when is_pid(pid) ->
        GenServer.call(pid, :stop_subscribers_and_workers)
    end
  end

  ## ==========================================================================
  ## Callbacks
  ## ==========================================================================
  def init([subscriber_name, consumer_group, worker_manager_pid, topic]) do
    :ok = kafka().start_consumer(subscriber_name, topic, [])

    {:ok, pid} =
      group_coordinator().start_link(
        subscriber_name,
        consumer_group,
        [topic],
        group_config(),
        __MODULE__,
        self()
      )

    Logger.info("event#init=#{__MODULE__}
       group_coordinator=#{inspect(pid)}
       subscriber_name=#{subscriber_name}
       consumer_group=#{consumer_group}")

    {:ok,
     %State{
       subscriber_name: subscriber_name,
       group_coordinator_pid: pid,
       consumer_group: consumer_group,
       worker_manager_pid: worker_manager_pid,
       topic: topic
     }}
  end

  # Handle the partition assignments. Wait the configured duration before allocating the
  # subscribers to give each consumer a chance to handle the latest generation of the
  # configuration.
  def handle_cast({:assignments_received, gen_id, assignments}, state) do
    Logger.info("event#assignments_received=#{name(state.subscriber_name, state.topic)} generation_id=#{gen_id}")

    Process.send_after(self(), {:allocate_subscribers, gen_id, assignments}, rebalance_delay())
    {:noreply, %{state | current_gen_id: gen_id}}
  end

  def handle_cast({:assignments_revoked}, state) do
    Logger.info("event#assignments_revoked=#{name(state.subscriber_name, state.topic)}")

    stop_subscribers(state.subscribers)
    {:noreply, %{state | :subscribers => []}}
  end

  # If we're not at the latest generation, discard the assignment for whatever is next.
  def handle_info({:allocate_subscribers, gen_id, _assignments}, %{current_gen_id: current_gen_id} = state)
      when gen_id < current_gen_id do
    Logger.debug("Discarding old generation #{gen_id} for current generation: #{current_gen_id}")
    {:noreply, state}
  end

  # If we are at the latest, allocate a subscriber per partition.
  def handle_info({:allocate_subscribers, gen_id, assignments}, state) do
    Logger.info("event#allocate_subscribers=#{inspect(self())} generation_id=#{gen_id}")

    if state.subscribers != [] do
      # Did we try to allocate without deallocating? We'd like to know.
      Logger.info("event#subscribers_not_empty=#{inspect(self())}")
      stop_subscribers(state.subscribers)
    end

    subscribers =
      Enum.map(assignments, fn assignment ->
        Logger.debug("Allocating subscriber for assignment: #{inspect(assignment)}")

        {:brod_received_assignment, topic, partition, offset} = assignment
        worker_pid = worker_manager().worker_for(state.worker_manager_pid, topic, partition)

        {:ok, pid} =
          subscriber().subscribe(
            state.subscriber_name,
            state.group_coordinator_pid,
            worker_pid,
            gen_id,
            topic,
            partition,
            compute_offset(offset, configured_offset())
          )

        pid
      end)

    {:noreply, %{state | :subscribers => subscribers}}
  end

  def handle_call(:stop_subscribers_and_workers, _from, state) do
    subscriber_mod = subscriber()
    worker_manager_mod = worker_manager()

    Enum.each(state.subscribers, fn s ->
      %{topic: topic, partition: partition} = subscriber_mod.status(s)
      :ok = worker_manager_mod.stop_worker_for(state.worker_manager_pid, topic, partition)
      :ok = subscriber_mod.stop(s)
    end)

    {:reply, :ok, %{state | subscribers: []}}
  end

  ## ==========================================================================
  ## Helpers
  ## ==========================================================================
  defp stop_subscribers(subscribers) do
    Enum.each(subscribers, fn s ->
      subscriber().stop(s)
    end)
  end

  defp compute_offset(:undefined, configured_offset) do
    [begin_offset: configured_offset]
  end

  defp compute_offset(offset, _configured_offset) do
    [begin_offset: offset]
  end

  defp configured_offset do
    Kaffe.Config.Consumer.begin_offset()
  end

  defp rebalance_delay do
    Kaffe.Config.Consumer.configuration().rebalance_delay_ms
  end

  defp group_config do
    Kaffe.Config.Consumer.configuration().group_config
  end

  defp kafka do
    Application.get_env(:kaffe, :kafka_mod, :brod)
  end

  defp group_coordinator do
    Application.get_env(:kaffe, :group_coordinator_mod, :brod_group_coordinator)
  end

  defp worker_manager do
    Application.get_env(:kaffe, :worker_manager_mod, Kaffe.WorkerManager)
  end

  defp subscriber do
    Application.get_env(:kaffe, :subscriber_mod, Kaffe.Subscriber)
  end

  defp name(subscriber_name, topic) do
    :"#{__MODULE__}.#{subscriber_name}.#{topic}"
  end
end
