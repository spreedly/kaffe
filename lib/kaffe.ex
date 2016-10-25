defmodule Kaffe do
  def start_brod_consumer(endpoints, name) do
    :brod.start_client(endpoints, name, consumer_config)
  end

  def start_brod_producer(endpoints, name) do
    :brod.start_client(endpoints, name, producer_config)
  end

  def consumer_config do
    [
      auto_start_producers: false,
      allow_topic_auto_creation: false
    ]
  end

  def producer_config do
    [
      auto_start_producers: true,
      allow_topic_auto_creation: false,
      default_producer_config: [
        required_acks: -1
      ]
    ]
  end
end
