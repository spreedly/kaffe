use Mix.Config

config :kaffe,
  kafka_mod: :brod,
  group_subscriber_mod: :brod_group_subscriber

import_config "#{Mix.env}.exs"
