defmodule Kaffe.ConsumerTest do
  use ExUnit.Case, async: true

  require Kaffe.Consumer
  alias Kaffe.Consumer

  setup do
    %{
      topic: "topic",
      partition: 0,
      consumer_state: %{async: false, message_handler: SilentMessage},
      message: Consumer.kafka_message(key: "", value: "message")
    }
  end

  test "when async is false messages are acknowledged by the response", c do
    state = %{c.consumer_state | async: false}
    assert {:ok, :ack, state} == Consumer.handle_message(c.topic, c.partition, c.message, state)
  end

  test "when async is true messages are acknowledged by the response", c do
    state = %{c.consumer_state | async: true}
    assert {:ok, state} == Consumer.handle_message(c.topic, c.partition, c.message, state)
  end

  test "kafka messages are compiled into a map including topic and partition", c do
    topic = c.topic
    partition = c.partition
    value = Consumer.kafka_message(c.message, :value)
    state = %{c.consumer_state | message_handler: SendMessage}

    Consumer.handle_message(c.topic, c.partition, c.message, state)
    assert_receive %{topic: ^topic, partition: ^partition, value: ^value}
  end
end
