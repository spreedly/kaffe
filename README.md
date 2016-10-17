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

`Kaffe.Consumer` is expected to be a supervised process that consumes messages from a Kafka topic/partition using an already running brod client. In the future it will also support starting a brod client itself as needed.

  1. Add a `handle_message/1` function to a local module. This function will be called with each Kafka message as a map.

    ```elixir
    %{attributes: 0, crc: -1821454227, key: "message key", magic_byte: 0, offset: 7, value: "message value"}
    ```

  2. Add `Kaffe.Consumer` as a worker in your supervision tree

    ```elixir
    # worker(Kaffe.Consumer, [brod_client, topic, partition, consumer_config, message_handler, async])
    worker(Kaffe.Consumer, [:brod_client_1, "commitlog", :all, [], MessageWorker, false])
    ```

    In this example `:brod_client_1` will be used to consume messages from `:all` partitions of the "commitlog" topic and call `MessageWorker.handle_message/1`. The Kafka messages will be automatically acknowledged when the `MessageWorker.handle_message/1` function completes.

    Future versions of Kaffe will support asynchronous message acknowledgement.

