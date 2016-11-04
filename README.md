# Kaffe

An opinionated, highly specific, Elixir wrapper around brod: the Erlang Kafka client. :coffee:

## Installation

  1. Add `kaffe` to your list of dependencies in `mix.exs`:

    ```elixir
    def deps do
      [{:kaffe, git: "git@github.com:spreedly/kaffe.git", branch: "master"}]
    end
    ```

  2. Ensure `kaffe` is started with your application:

    ```elixir
    def application do
      [applications: [:logger, :kaffe]]
    end
    ```

  3. Configure Kaffe with your Kafka endpoints

    Each endpoint is `hostname: port`

    ```elixir
    config :kaffe,
      kafka_consumer_endpoints: [kafka: 9092],
      kafka_producer_endpoints: [kafka: 9092]
    ```

    If you only need to consume or produce then you only need to configure those respective endpoints.

## Usage

Kaffe provides two modules: `Kaffe.Consumer` and `Kaffe.Producer`.

### Consumer

`Kaffe.Consumer` is expected to be a supervised process that consumes messages from a Kafka topic or list of topics.

  1. Add a `handle_message/1` function to a local module (e.g. `MessageProcessor`). This function will be called with each Kafka message as a map. Each message map will include the topic and partition in addition to the normal Kafka message metadata.

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

    Required arguments:

    - `consumer_group` - the consumer group id (should be unique to your app)
    - `topics` - the list of Kafka topics or a single topic to consume
    - `message_handler` - the module that will be called for each Kafka message

    Optional:

    - `async` - if false then Kafka messages are automatically acknowledged after handling is complete (default: `false`)

    Example:

    ```elixir
    consumer_group = "demo-commitlog-consumer"
    topic = "commitlog"
    message_handler = MessageProcessor

    worker(Kaffe.Consumer, [consumer_group, topic, message_handler])
    ```

    ```elixir
    defmodule MessageProcessor
      def handle_message(%{key: key, value: value} = message) do
        IO.inspect message
        IO.puts "#{key}: #{value}"
        :ok # The handle_message function MUST return :ok
      end
    end
    ```

    In that example Kaffe will consume messages from the "commitlog" topic and call `MessageWorker.handle_message/1` with each message. The Kafka messages will be automatically acknowledged when the `MessageWorker.handle_message/1` function returns `:ok`.

#### async message acknowledgement

If you need asynchronous message consumption:

  1. Add a `handle_message/2` function to your processing module. This function will be called with the Consumer `pid` and the Kafka message. When your processing is complete you will need to call `Kaffe.Consumer.ack(pid, message)` to acknowledge the offset.

  2. Set `async` to true when you start the Kaffe.Consumer

    ```elixir
    consumer_group = "demo-commitlog-consumer"
    topic = "commitlog"
    message_handler = MessageProcessor
    async = true

    worker(Kaffe.Consumer, [consumer_group, topics, message_handler, async])

    # … in your message handler module

    def handle_message(pid, message) do
      spawn_message_processing_worker(pid, message)
      :ok # MUST return :ok
    end

    # … somewhere in your system when the worker is finished processing

    Kaffe.Consumer.ack(pid, message)
    ```

**NOTE**: Asynchronous consumption means your system will no longer provide any backpressure to the Kaffe.Consumer. You will also need to add robust measures to your system to ensure that no messages are lost in processing. IE if you spawn 5 workers processing a series of asynchronous messages from Kafka and 1 of them crashes without acknowledgement then it's possible and likely that the message will be skipped entirely.

Kafka only tracks a single numeric offset, not individual messages. If a message fails and a later offset is committed then the failed message will _not_ be sent again.

It's possible that your topic and system are entirely ok with losing some messages (i.e. frequent metrics that aren't individually important).

### Producer

`Kaffe.Producer` is expected to be a supervised process. It handles producing messages to Kafka and will automatically select the topic partitions per message or can be given a function to call to determine the partition per message.

  1. Add `Kaffe.Producer` as a worker in your supervision tree.

  Required arguments:

  - `topics` or `topic` - a list of topics or a single topic

  Optional:

  - `strategy` - a partition selection strategy (default: `:round_robin`)
    - `:round_robin` - cycle through each partition
    - `:random` - select a random partition
    - function - a given function to call to determine the correct partition

  Examples

    ```elixir
    # prepare for producing to the whitelist topic using round robin partitioning
    Kaffe.Producer.start_link("whitelist")

    # prepare for producing to the whitelist topic using random partitioning
    Kaffe.Producer.start_link("whitelist", :random)

    # prepare for producing to the whitelist topic using a local function
    # assuming KafkaMeta.choose_partition/5 is locally defined

    defmodule KafkaMeta do
      def choose_partition(topic, current_partition, partitions_count, key, value) do
        # some calculation to return an integer between 0 and partitions_count-1
      end
    end

    Kaffe.Producer.start_link("whitelist", &KafkaMeta.choose_partition/5)
    ```

#### Configuration Examples

```elixir
topics = ["output1", "output2"]
worker(Kaffe.Producer, [topics])
```

```elixir
topic = "whitelist"
worker(Kaffe.Producer, [topic])
```

```elixir
topic = "whitelist"
worker(Kaffe.Producer, [topic, :random])
```

```elixir
topic = "whitelist"
worker(Kaffe.Producer, [topic, &Producer.choose_partition/5])
```

#### Usage Examples

Currently only synchronous message production is supported.

There are three ways to produce:

- `key`/`value` - The key/value will be produced to the first topic given to the producer when it was started. The partition will be selected with the chosen strategy or given function.
    ```elixir
    Kaffe.Producer.produce_sync("key", "value")
    ```

- `topic`/`key`/`value` - The key/value will be produced to the given topic.

    ```elixir
    Kaffe.Producer.produce_sync("whitelist", "key", "value")
    ```

- `topic`/`partition`/`key`/`value` - The key/value will be produced to the given topic/partition.

    ```elixir
    Kaffe.Producer.produce_sync("whitelist", 2, "key", "value")
    ```

    **NOTE**: With this approach Kaffe will not calculate the next partition since it assumes you're taking over that job by giving it a specific partition.

