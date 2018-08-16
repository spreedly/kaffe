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

    if Application.get_env(:kaffe, :producer) do
      Logger.debug("event#start_producer_client=#{__MODULE__}")
      Kaffe.Producer.start_producer_client()
    end

    children = []

    opts = [strategy: :one_for_one, name: Kaffe.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
