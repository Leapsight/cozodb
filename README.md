cozodb
=====

Erlang NIF bindings for CozoDB using rustler.


## Requirements
* Erlang OTP26 and/or Elixir (latest)
* Rust
* Linux
  * `liburing-dev`
  * `pkg-config`

## Installation

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
        },

    ]
```
