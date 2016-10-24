use Mix.Config

config :brod, [
  clients: [
    brod_client_1: [
      auto_start_producers: true,
      endpoints: [kafka: 9092],
    ]
  ]
]
