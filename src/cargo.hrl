-cargo_header_version(1).
-ifndef(CARGO_LOAD_APP).
-define(CARGO_LOAD_APP, cozodb).
-endif.
-ifndef(CARGO_HRL).
-define(CARGO_HRL, 1).

%% Try to load precompiled NIF first, fall back to source-compiled NIF.
%% This macro wraps cozodb_nif_loader for backward compatibility.
-define(load_nif_from_crate(__CRATE, __INIT),
    (fun() ->
        __APP = ?CARGO_LOAD_APP,
        __PATH = filename:join([code:priv_dir(__APP), "crates", __CRATE, __CRATE]),
        case filelib:is_file(__PATH ++ ".so") orelse
             filelib:is_file(__PATH ++ ".dylib") of
            true ->
                %% NIF already exists (either precompiled or source-built)
                erlang:load_nif(__PATH, __INIT);
            false ->
                %% Try to download precompiled, then load
                case cozodb_nif_loader:load_nif() of
                    ok -> ok;
                    {error, _} ->
                        %% Final fallback: try loading from source path anyway
                        erlang:load_nif(__PATH, __INIT)
                end
        end
    end)()
).

%% Legacy macro for direct source loading (skips precompiled download)
-define(load_nif_from_crate_source(__CRATE, __INIT),
    (fun() ->
        __APP = ?CARGO_LOAD_APP,
        __PATH = filename:join([code:priv_dir(__APP), "crates", __CRATE, __CRATE]),
        erlang:load_nif(__PATH, __INIT)
    end)()
).
-endif.
