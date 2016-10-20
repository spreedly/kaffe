# Kaffe

An opinionated, highly specific, Elixir wrapper around brod: the Erlang Kafka client. :coffee:

## Installation

  1. Add `kaffe` to your list of dependencies in `mix.exs`:

    ```elixir
    def deps do
      [{:kaffe, github: "spreedly/kaffe", branch: "master"}]
    end
    ```

  2. Ensure `kaffe` is started with your application:

    ```elixir
    def application do
      [applications: [:logger, :kaffe]]
    end
    ```

  3. Configure at least one `brod` client in your application:

    ```elixir
	config :brod, [
	  clients: [
		brod_client: [
		  auto_start_producers: true,
		  endpoints: [kafka: 9092],
		]
	  ]
	]
    ```

## Usage

Kaffe provides two modules: `Kaffe.Consumer` and `Kaffe.Producer`.

### Consumer

`Kaffe.Consumer` is expected to be a supervised process that consumes messages from a Kafka topic/partition using an already running brod client.

  1. Add a `handle_message/1` function to a local module (e.g. `MessageProcessor`). This function will be called with each Kafka message as a map. Because we're using a consumer group the message map will include the topic and partition.

    ```elixir
    %{
      attributes: 0,
      crc: 1914336469,
      key: "",
      magic_byte: 0,
      offset: 41,
      partition: 0,
      topic: "sdball",
      value: "awesome24\n"
    }
    ```

  2. Add `Kaffe.Consumer` as a worker in your supervision tree

    The Consumer requires several arguments:

    - `client`: the id of an active brod client to use for consuming
    - `consumer_group`: the consumer group id (should be unique to your app)
    - `topics`: the list of Kafka topics to consume
    - `message_handler`: the module that will be called for each Kafka message

    Optional:

    - `async`: if false then Kafka messages are automatically acknowledged after handling is complete (default: `false`)

    Example:

    ```elixir
    client = :brod_client
    topics = ["commitlog"]
    consumer_group = "demo-commitlog-consumer"
    message_handler = MessageProcessor

    worker(Kaffe.Consumer, [client, consumer_group, topics, message_handler])
    ```

    ```elixir
    defmodule MessageProcessor
      def handle_message(%{key: key, value: value} = message) do
        IO.inspect message
        IO.puts "#{key}: #{value}"
        :ok
      end
    end
    ```

    In that example `:brod_client` will be used to consume messages from the "commitlog" topic and call `MessageWorker.handle_message/1` with each message. The Kafka messages will be automatically acknowledged when the `MessageWorker.handle_message/1` function returns `:ok`.

#### async message acknowledgement

If you need asynchronous message consumption:

  1. Add a `handle_message/2` function to your processing module. This function will be called with the Consumer `pid` and the Kafka message. When your processing is complete you will need to call `Kaffe.Consumer.ack(pid, message)` to acknowledge the offset.

  2. Set `async` to true when you start the Kaffe.Consumer

    ```elixir
    client = :brod_client
    topics = ["commitlog"]
    consumer_group = "demo-commitlog-consumer"
    message_handler = MessageProcessor
    async = true

    worker(Kaffe.Consumer, [client, consumer_group, topics, message_handler, async])

    # … in your message handler module

    def handle_message(pid, message) do
      spawn_message_processing_worker(pid, message)
      :ok
    end

    # … somewhere in your system when the worker is finished processing

    Kaffe.Consumer.ack(pid, message)
    ```

**NOTE**: Asynchronous consumption means your system will no longer provide any backpressure to the Kaffe.Consumer. You will also need to add robust measures to your system to ensure that no messages are lost in processing. IE if you spawn 5 workers processing a series of asynchronous messages from Kafka and 1 of them crashes without acknowledgement then it's possible and likely that the message will be skipped entirely.

Kafka only tracks a single numeric offset, not individual messages. If a message fails and a later offset is committed then the failed message will _not_ be sent again.

It's possible that your topic and system are entirely ok with losing some messages (i.e. frequent metrics that aren't individually important).

### Producer

`Kaffe.Producer` is also a supervised process that will handle automatically selecting the partition to produce to.

  1. Add `Kaffe.Producer` as a worker in your supervision tree.

  Required arguments:

  - `client`: a running brod client configured to produce
  - `topics` or `topic`: a list of topics or a single topic string

  Optional:

  - `strategy`: a partition selection strategy (default: `:round_robin`)

#### Configuration Examples

```elixir
client = :brod_client_1
topics = ["output1", "output2"]
worker(Kaffe.Producer, [client, topics])
```

```elixir
client = :brod_client_1
topic = "whitelist"
worker(Kaffe.Producer, [client, topic])
```

#### Usage Examples

Currently only synchronous message production is supported.

There are three ways to produce:

- `key`/`value`: The key/value will be produced to the first topic given to the producer when it was started. The partition will automatically be selected with the chosen strategy.
    ```elixir
    Kaffe.Producer.produce_sync("key", "value")
    ```

- `topic`/`key`/`value`: The key/value will be produced to the given topic.

    ```elixir
    Kaffe.Producer.produce_sync("whitelist", "key", "value")
    ```

- `topic`/`partition`/`key`/`value`: The key/value will be produced to the given topic/partition.

    ```elixir
    Kaffe.Producer.produce_sync("whitelist", 2, "key", "value")
    ```

