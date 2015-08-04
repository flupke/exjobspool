defmodule Jobspool.Mixfile do
  use Mix.Project

  def project do
    [
      app: :jobspool,
      version: "0.0.2",
      description: "Simple Elixir jobs pool",
      elixir: "~> 1.0",
      build_embedded: Mix.env == :prod,
      start_permanent: Mix.env == :prod,
      deps: deps,
      package: package,
    ]
  end

  # Configuration for the OTP application
  #
  # Type `mix help compile.app` for more information
  def application do
    [applications: [:logger]]
  end

  # Dependencies can be Hex packages:
  #
  #   {:mydep, "~> 0.3.0"}
  #
  # Or git/path repositories:
  #
  #   {:mydep, git: "https://github.com/elixir-lang/mydep.git", tag: "0.1.0"}
  #
  # Type `mix help deps` for more examples and options
  defp deps do
    [
      {:uuid, "~> 1.0"},

      {:earmark, "~> 0.1", only: :dev},
      {:ex_doc, "~> 0.7", only: :dev},
    ]
  end

  defp package do
    [
      contributors: ["Luper Rouch"],
      licenses: ["MIT"],
      links: %{"Github" => "https://github.com/flupke/exjobspool"}
    ]
  end
end
