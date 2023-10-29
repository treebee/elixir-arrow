defmodule ArrowElixir.MixProject do
  use Mix.Project

  def project do
    [
      app: :arrow,
      version: "0.2.0",
      elixir: "~> 1.11",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      rustler_crates: rustler_crates(),
      compilers: Mix.compilers(),
      # Docs
      name: "Arrow",
      source_url: "https://github.com/treebee/elixir-arrow",
      docs: [
        main: "Arrow",
        extras: ["README.md"]
      ],
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.post": :test,
        "coveralls.html": :test
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :eex]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:rustler, "~> 0.30"},
      {:toml, "~> 0.7"},
      {:ex_doc, "~> 0.27", only: :dev, runtime: false},
      {:ex_parameterized, "~> 1.3.7", only: :test},
      {:excoveralls, "~> 0.18", only: :test}
    ]
  end

  defp rustler_crates() do
    [
      arrow_nif: [
        path: "native/arrow_nif",
        mode: rustc_mode(Mix.env())
      ]
    ]
  end

  defp rustc_mode(:prod), do: :release
  defp rustc_mode(_), do: :debug
end
