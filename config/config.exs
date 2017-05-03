use Mix.Config

config :kaffe,
  kafka_mod: :brod,
  group_subscriber_mod: :brod_group_subscriber,
  group_coordinator_mod: :brod_group_coordinator,
  worker_manager_mod: Kaffe.WorkerManager,
  subscriber_mod: Kaffe.Subscriber

# config :logger, :console,
#   level: :debug,
#   format: "$date $time $metadata[$level] $message\n",
#   metadata: [:module]
config :logger, backends: []

import_config "#{Mix.env}.exs"
