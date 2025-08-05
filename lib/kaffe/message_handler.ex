defmodule Kaffe.MessageHandler do
  @moduledoc """
  The behaviour for a message handler.

  The module implementing this behaviour needs to be configured for the consumer
  under Kaffe config.

  ```
  config :kaffe,
        consumers: %{
          "subscriber_1" => [
            ...
            message_hander: MyApp.MessageHandler
          ]
        }
  ```
  """
  alias Kaffe.Consumer

  @doc """
  The functionality responsible for handling the message set from Kaffe.

  Each message will include the topic and partition in addition to the normal Kafka
  message metadata.

  The response from the message handler is what dictates how a
  subscriber should deal with the message offset. Depending on the situation,
  a message processor may not want to have it's most recent offsets committed.

  In some cases you may not want to commit back the most recent offset after
  processing a list of messages. For example, if you're batching messages to be
  sent elsewhere and want to ensure that a batch can be rebuilt should there be
  an error further downstream. In that example you might want to keep the offset
  of the first message in your batch so your consumer can restart back at that point
  to reprocess and rebatch the messages.

  The following returns are supported:
   - `:ok` - commit back the most recent offset and request more messages
   - `{:ok, :no_commit}` - do _not_ commit back the most recent offset and
      request more messages from the offset of the last message
   - `{:ok, offset}` - commit back at the offset specified and request
      messages from that point forward

  Because the return types are only success based, the message handler needs
  to handle errors.
  """
  @callback handle_messages(messages :: list(Consumer.message())) ::
              :ok | {:ok, :no_commit} | {:ok, offset :: :brod.offset()}
end
