defmodule Kaffe do
  @moduledoc """
  An opinionated, highly specific, Elixir wrapper around brod: the Erlang Kafka client. :coffee:

  **NOTE**: Although we're using this in production at Spreedly it is still under active development. The API may change and there may be serious bugs we've yet to encounter.
  """

  use Application

  require Logger

  def start(_type, _args) do
    import Supervisor.Spec, warn: false

    Logger.debug("event#start=#{__MODULE__}")

    Kaffe.Config.Producer.maybe_set_producers_env!()

    if producers = Application.get_env(:kaffe, :producers) do
      producers
      |> Enum.map(&elem(&1, 0))
      |> Enum.each(fn config_key ->
        Logger.debug("event#start_producer_client=#{__MODULE__}_#{config_key}")
        config = Kaffe.Config.Producer.configuration(config_key)
        Kaffe.Producer.start_producer_client(config)
      end)
    end

    children = []

    opts = [strategy: :one_for_one, name: Kaffe.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
