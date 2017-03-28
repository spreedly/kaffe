defmodule Kaffe.GroupMember do
  @moduledoc """
  Consume messages from a Kafka topic for a consumer group.

  The actual consumption is delegated to a series of subscribers, see
  `Kaffe.Subscriber`.

  The subscribers are assigned generations. Each generation represents a
  specific configuration. In order to allow the partitions to be rebalanced on
  startup, there is a delay between receiving a set of assignments associated
  with that generation and actually creating the subscribers. If a new
  generation is received in the mean time, the older generation is discarded.

  See: https://github.com/klarna/brod/blob/master/src/brod_group_member.erl

  Also: https://github.com/klarna/brucke/blob/master/src/brucke_member.erl

  The `brod_group_member` behavior is used. 
  """

  use GenServer

  @behaviour :brod_group_member

  alias Kaffe.WorkerManager

  require Logger

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

  def start_link(subscriber_name, consumer_group, worker_manager_pid, topic, configured_offset) do
    GenServer.start_link(__MODULE__, [subscriber_name, consumer_group,
      worker_manager_pid, topic, configured_offset])
  end

  def init([subscriber_name, consumer_group, worker_manager_pid, topic, configured_offset]) do

    :ok = kafka().start_consumer(subscriber_name, topic, [])
    {:ok, pid} = group_coordinator().start_link(subscriber_name, consumer_group,
      [topic], _group_config = [], __MODULE__, self())

    Logger.info "event#init group_coordinator=#{inspect pid} subscriber_name=#{subscriber_name} consumer_group=#{consumer_group}"

    {:ok, %State{subscriber_name: subscriber_name,
                group_coordinator_pid: pid,
                consumer_group: consumer_group,
                worker_manager_pid: worker_manager_pid,
                topic: topic,
                configured_offset: configured_offset}}
  end

  def get_committed_offsets(_group_member_pid, _topic_partitions) do
    # Should not receive this
    Logger.warn "status#get_committed_offsets"
  end

  def assign_partitions(_pid, _members, _topic_partitions) do
    # Should not receive this
    Logger.warn "status#assign_partitions"
  end

  def assignments_received(pid, _member_id, generation_id, assignments) do
    GenServer.cast(pid, {:assignments_received, generation_id, assignments})
  end

  def assignments_revoked(pid) do
    GenServer.cast(pid, {:assignments_revoked})
  end

  # Handle the partition assignments. Wait the configured duration before allocating the
  # subscribers to give each consumer a chance to handle the latest generation of the
  # configuration.
  def handle_cast({:assignments_received, gen_id, assignments}, state) do
    Logger.info "event#assignments_received=#{gen_id}"
    Process.send_after(self(), {:allocate_subscribers, gen_id, assignments}, rebalance_delay())
    {:noreply, %{state | current_gen_id: gen_id}}
  end
  def handle_cast({:assignments_revoked}, state) do
    Logger.info "event#assignments_revoked"
    Enum.each(state.subscribers, fn (s) ->
      Logger.debug "Stopping subscriber: #{inspect s}"
      Kaffe.Subscriber.stop(s)
    end)

    {:noreply, %{state | :subscribers => []}}
  end

  # If we're not at the latest generation, discard the assignment for whatever is next.
  def handle_info({:allocate_subscribers, gen_id, _assignments}, %{current_gen_id: current_gen_id} = state)
      when gen_id < current_gen_id do
    Logger.info "Discarding old generation #{gen_id} for current generation: #{current_gen_id}"
    {:noreply, state}
  end
  # If we are at the latest, allocate a subscriber per partition.
  def handle_info({:allocate_subscribers, gen_id, assignments}, state) do

    Logger.info "event#allocate_subscribers=#{inspect self()} generation_id=#{gen_id}"

    subscribers = Enum.map(assignments, fn (assignment) ->

      {:brod_received_assignment, topic, partition, offset} = assignment

      worker_pid = WorkerManager.worker_for(state.worker_manager_pid, partition)

      {:ok, pid} = subscriber().subscribe(
        state.subscriber_name,
        state.group_coordinator_pid,
        worker_pid,
        gen_id,
        topic,
        partition,
        compute_offset(offset, state.configured_offset))

      pid
    end)

    {:noreply, %{state | :subscribers => subscribers}}
  end

  defp compute_offset(:undefined, configured_offset) do
    [begin_offset: configured_offset]
  end
  defp compute_offset(offset, _configured_offset) do
    [begin_offset: offset]
  end

  defp rebalance_delay do
    Kaffe.Config.Consumer.configuration.rebalance_delay_ms
  end

  defp group_coordinator do
    Application.get_env(:kaffe, :group_coordinator_mod, :brod_group_coordinator)
  end

  defp kafka do
    Application.get_env(:kaffe, :kafka_mod, :brod)
  end

  defp subscriber do
    Application.get_env(:kaffe, :subscriber_mod, Kaffe.Subscriber)
  end

end
