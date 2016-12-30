defmodule Kaffe do
  def start_consumer_client(config) do
    :brod.start_client(config.endpoints, config.subscriber_name, config.consumer_config)
  end

  def start_producer_client(config) do
    :brod.start_client(config.endpoints, config.client_name, config.producer_config)
  end
end
