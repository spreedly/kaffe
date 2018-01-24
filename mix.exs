defmodule Kaffe.Mixfile do
  use Mix.Project

  def project do
    [app: :kaffe,
     version: "1.6.0",
     description: "An opinionated Elixir wrapper around brod, the Erlang Kafka client, that supports encrypted connections to Heroku Kafka out of the box.",
     name: "Kaffe",
     source_url: "https://github.com/spreedly/kaffe",
     package: package(),
     elixir: "~> 1.3",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     elixirc_paths: elixirc_paths(Mix.env),
     deps: deps()]
  end

  def application do
    [applications: [:logger, :brod],
     mod: {Kaffe, []}]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_env), do: ["lib"]

  defp deps do
    [
      {:brod, "~> 3.0"},
      {:ex_doc, "~> 0.14", only: :dev, runtime: false},
    ]
  end

  defp package do
    [
      name: :kaffe,
      licenses: ["MIT License"],
      maintainers: ["Stephen Ball", "Kevin Lewis", "Ryan Daigle", "Spreedly"],
      links: %{"GitHub" => "https://github.com/spreedly/kaffe"}
    ]
  end
end
