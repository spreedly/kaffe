# Kaffe

An Elixir wrapper around brod, the Erlang Kafka client.

## Installation

  1. Add `kaffe` to your list of dependencies in `mix.exs`:

    ```elixir
    def deps do
      [{:kaffe, github: "spreedly/kaffe", tag: "master"}]
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
		brod_client_1: [
		  auto_start_producers: true,
		  endpoints: [kafka: 9092],
		  config: []
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
    - `async`: if false then Kafka messages are acknowledged after handling is complete

    Example:

    ```elixir
    client = :brod_client
    topics = ["commitlog"]
    consumer_group = "demo-commitlog-consumer"
    message_handler = MessageProcessor
    async = false

    worker(Kaffe.Consumer, [client, consumer_group, topics, message_handler, async])
    ```

    In this example `:brod_client` will be used to consume messages from the "commitlog" topic and call `MessageWorker.handle_message/1` with each message. The Kafka messages will be automatically acknowledged when the `MessageWorker.handle_message/1` function completes.

#### async message acknowledgement

If you want to asynchronously acknowledge message offsets then set `async` to `true` and either keep track of the pid for the Consumer process or your `consumer_group` name.

After messages have processed call `Kaffe.Consumer.ack/4` with either `pid, topic, partition, offset` or `consumer_group, topic, partition, offset`.

e.g.

```elixir
partition = 0
offset = 147
Kaffe.Consumer.ack("commitlog-index-group", "commitlog", partition, offset)
```
