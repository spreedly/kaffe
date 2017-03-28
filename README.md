# Kaffe

An opinionated, highly specific, Elixir wrapper around brod: the Erlang Kafka client. :coffee:

**NOTE**: Although we're using this in production at Spreedly it is still under active development. The API may change and there may be serious bugs we've yet to encounter.

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

  3. Configure a Kaffe Consumer and/or Producer

## Kaffe Consumer Usage

Kaffe has two primary modes of message consumption.

Single message consumers process one message at a time using the `:brod_group_subscriber` behavior. This supports consumers in a consumer group in a single node (e.g., Heroku dyno). For such consumers, this is the place to start as it's the simplest mode of operation.

Batch message consumers receive a list of messages and work as part of the `:brod_group_member` behavior. This has a few important benefits:

1. Group members assign a "subscriber" to each partition in the topic. Because Kafka topics scale with partitions, having a worker per partition usually increases throughput.
2. Group members correctly handle partition assignments across multiple clients in a consumer group. This means that this mode of operation will scale horizontally (e.g., multiple dynos on Heroku).
3. Downstream processing that benefits from batching (like writing to another Kafka topic) is more easily supported.

### Kaffe Consumer - Single Message Consumer

1. Add a `handle_message/1` function to a local module (e.g. `MessageProcessor`). This function will be called with each Kafka message as a map. Each message map will include the topic and partition in addition to the normal Kafka message metadata.

    The module's `handle_message/1` function _must_ return `:ok` or Kaffe will throw an error. In normal (synchronous consumer) operation the Kaffe consumer will block until your `handle_message/1` function returns `:ok`.

    ### Example

      ```elixir
      defmodule MessageProcessor
        def handle_message(%{key: key, value: value} = message) do
          IO.inspect message
          IO.puts "#{key}: #{value}"
          :ok # The handle_message function MUST return :ok
        end
      end
      ```

    ### Message Structure

    ```elixir
    %{
      attributes: 0,
      crc: 1914336469,
      key: "kafka message key",
      magic_byte: 0,
      offset: 41,
      partition: 17,
      topic: "some-kafka-topic",
      value: "the actual kafka message value is here"
    }
    ```

2. Configure your Kaffe Consumer in your mix config

      ```elixir
      config :kaffe,
        consumer: [
          endpoints: [kafka: 9092], # that's [hostname: kafka_port]
          topics: ["interesting-topic"], # the topic(s) that will be consumed
          consumer_group: "your-app-consumer-group", # the consumer group for tracking offsets in Kafka
          message_handler: MessageProcessor, # the module from Step 1 that will process messages

          # optional
          async_message_ack: false, # see "async message acknowledgement" below
          start_with_earliest_message: true # default false
        ],
      ```

    The `start_with_earliest_message` field controls where your consumer group starts when it starts for the very first time. Once offsets have been committed to Kafka then they will supercede this option. If omitted then your consumer group will start processing from the most recent messages in the topic instead of consuming all available messages.

    ### Heroku Configuration

    To configure a Kaffe Consumer for a Heroku Kafka compatible environment including SSL omit the `endpoint` and instead set `heroku_kafka_env: true`

    ```elixir
    config :kaffe,
      consumer: [
        heroku_kafka_env: true,
        topics: ["interesting-topic"],
        consumer_group: "your-app-consumer-group",
        message_handler: MessageProcessor
      ]
    ```

    With that setting in place Kaffe will automatically pull required info from the following ENV variables:

    - `KAFKA_URL`
    - `KAFKA_CLIENT_CERT`
    - `KAFKA_CLIENT_CERT_KEY`
    - `KAFKA_TRUSTED_CERT` (not used yet)

3. Add `Kaffe.Consumer` as a worker in your supervision tree

      ```elixir
      worker(Kaffe.Consumer, [])
      ```

### Kaffe GroupMember - Batch Message Consumer

  1. There are two modes for using the batch consumer. The first enable an easy migration path from using the `Kaffe.Consumer`. The second is more batch oriented, which allows consumers to handle a group of messages for efficiency or greater context.
  
     `handle_message/1` This is the same as for `Kaffe.Consumer`

     `handle_messages/1` This function (note the pluralization) will be called with a *list of messages*, with each message as a map. Each message map will include the topic and partition in addition to the normal Kafka message metadata.

     The module's `handle_messages/1` function _must_ return `:ok` or Kaffe will throw an error. The Kaffe consumer will block until your `handle_messages/1` function returns `:ok`.

  2. The configuration options for the GroupMember consumer are a superset of those for `Kaffe.Consumer`. The additional options are:

     `:rebalance_delay_ms` which is the time to allow for rebalancing
     among workers. The default is 10,000, which should give the
     consumers time to rebalance when scaling.

     `:max_bytes` limits the number of message bytes received from Kafka
     for a particular topic subscriber. The default is 1MB. This
     parameter might need tuning depending on the number of partitions
     in the topics being read (there is one subscriber per topic per
     partition). For example, if you are reading from two topics, each
     with 32 partitions, there is the potential of 64MB in buffered
     messages at any one time.

  3. Add `Kaffe.GroupMemberSupervisor` as a supervisor in your
     supervision tree

    ```elixir
    supervisor(Kaffe.GroupMemberSupervisor, [])
    ```

### async message acknowledgement

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

## Kaffe Producer Usage

`Kaffe.Producer` handles producing messages to Kafka and will automatically select the topic partitions per message or can be given a function to call to determine the partition per message.

1. Configure your Kaffe Producer in your mix config

      ```elixir
      config :kaffe,
        producer: [
          endpoints: [kafka: 9092], # [hostname: port]
          topics: ["kafka-topic"],

          # optional
          partition_strategy: :round_robin
        ]
      ```

    The `partition_strategy` setting can be one of:

    - `:md5`: (default) provides even and deterministic distrbution of the messages over the available partitions based on an MD5 hash of the key
    - `:round_robin`: cycle through each partition starting from 0 at application start
    - `:random`: select a random partition for each message
    - function: a given function to call to determine the correct partition

    ### Heroku Configuration

    To configure a Kaffe Producer for a Heroku Kafka compatible environment including SSL omit the `endpoint` and instead set `heroku_kafka_env: true`

    ```elixir
    config :kaffe,
      producer: [
        heroku_kafka_env: true,
        topics: ["kafka-topic"],

        # optional
        partition_strategy: :round_robin
      ]
    ```

    With that setting in place Kaffe will automatically pull required info from the following ENV variables:

    - `KAFKA_URL`
    - `KAFKA_CLIENT_CERT`
    - `KAFKA_CLIENT_CERT_KEY`
    - `KAFKA_TRUSTED_CERT`

2. Add `Kaffe.Producer` as a worker in your supervision tree.

      ```elixir
      worker(Kaffe.Producer, [])
      ```

### Producing to Kafka

Currently only synchronous message production is supported.

Once the `Kaffe.Producer` has started there are three ways to produce:

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


## Testing

### Setup

In order to run the end to end tests, a Kafka topic is required. It must:

* be named `kaffe-test`
* have 32 partitions

If using the `kafka-topics.sh` script that comes with the Kafka distribution, you may use something like:

```bash
kafka-topics.sh --zookeeper localhost:2181 --create --partitions 32 --replication-factor 1 --topic kaffe-test
```

### Running

```bash
# unit tests
mix test
# end to end test
mix test --only e2e
```
