defmodule Kaffe.Mixfile do
  use Mix.Project

  @source_url "https://github.com/spreedly/kaffe"
  @version "1.27.0"

  def project do
    [
      app: :kaffe,
      version: @version,
      name: "Kaffe",
      elixir: "~> 1.14",
      build_embedded: Mix.env() == :prod,
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      docs: docs(),
      package: package()
    ]
  end

  def application do
    [
      applications: [:logger, :brod, :retry],
      mod: {Kaffe, []}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_env), do: ["lib"]

  defp deps do
    [
      {:brod, "~> 3.0"},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false},
      {:retry, "~> 0.15.0"}
    ]
  end

  defp package do
    [
      name: :kaffe,
      description:
        "An opinionated Elixir wrapper around brod, the Erlang Kafka client, " <>
          "that supports encrypted connections to Heroku Kafka out of the box.",
      licenses: ["MIT"],
      maintainers: ["Kevin Lewis", "David Santoso", "Ryan Daigle", "Spreedly", "Joe Peck", "Brittany Hayes", "Anthony Walker"],
      links: %{
        "GitHub" => @source_url
      }
    ]
  end

  defp docs do
    [
      extras: [
        "README.md": [title: "Overview"],
        "LICENSE.md": [title: "License"]
      ],
      main: "readme",
      source_url: @source_url,
      formatters: ["html"]
    ]
  end
end
