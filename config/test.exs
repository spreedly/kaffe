use Mix.Config

config :kaffe,
  kafka_mod: TestBrod,
  group_subscriber_mod: TestBrodGroupSubscriber,
  test_partition_count: 17
