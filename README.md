cozodb
=====

Erlang/BEAM NIF bindings for CozoDB using Rustler.

CozoDB is a FOSS embeddable, transactional, relational-graph-vector database, with time travelling capability, perfect as the long-term memory for LLMs and AI.

## Installation

### Requirements
* Erlang OTP26 and/or Elixir (latest)
* Rust 1.76.0
* macOS packages: 
  * `liblz4`, 
  * `libssl`
* Linux packages:
  * `build-essential` 
  * `liblz4-dev` 
  * `libncurses-dev` 
  * `libsnappy-dev` 
  * `libssl-dev` 
  * `liburing-dev`
  * `liburing-dev` 
  * `liburing2`
  * `pkg-config`

### Erlang
Add the following to your `rebar.config` file.

```erlang
{deps, [
    {cozodb,
      {git, "https://github.com/leapsight/cozodb.git", {branch, "master"}}
    }
]}.
```

### Elixir
Add the following to your `mix.exs` file.

```elixir
  defp deps do
    [
        {:cozodb,
            git: "https://github.com/leapsight/cozodb.git",
            branch: "master"
        }
    ]
```
