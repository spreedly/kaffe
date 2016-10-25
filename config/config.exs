use Mix.Config

config :kaffe,
  kafka_mod: :brod,
  group_subscriber_mod: :brod_group_subscriber,
  kafka_consumer_endpoints: [kafka: 9092],
  kafka_producer_endpoints: [kafka: 9092]

import_config "#{Mix.env}.exs"
