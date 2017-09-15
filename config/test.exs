use Mix.Config

config :kaffe,
  kafka_mod: TestBrod,
  group_subscriber_mod: TestBrodGroupSubscriber,
  test_partition_count: 32,
  consumer: [
    endpoints: [kafka: 9092],
    topics: ["kaffe-test"],
    consumer_group: "kaffe-test-group",
    message_handler: SilentMessage,
    async_message_ack: false,
    offset_commit_interval_seconds: 10,
    start_with_earliest_message: true,
    rebalance_delay_ms: 100,
    max_bytes: 10_000,
    max_messages: 50,
    subscriber_retries: 1,
    subscriber_retry_delay_ms: 5,
  ],
  producer: [
    endpoints: [kafka: 9092],
    topics: ["kaffe-test"]
  ]
