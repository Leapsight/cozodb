-module(cozodb_nif_loader).

-export([
    load_nif/0,
    load_nif/1,
    get_platform_info/0,
    download_precompiled/1,
    verify_checksum/2
]).

%% GitHub repository for downloading precompiled NIFs
%% Can be overridden with environment variable COZODB_GITHUB_REPO
-define(DEFAULT_GITHUB_REPO, "adiibanez/cozodb").
-define(NIF_VERSION, "2.17").
-define(APP_NAME, cozodb).
-define(CRATE_NAME, "cozodb").

-type platform_info() :: #{
    os => linux | darwin | windows,
    arch => x86_64 | aarch64 | arm | riscv64,
    abi => gnu | musl | gnueabihf | darwin | msvc,
    target => binary()
}.

%% @doc Load the NIF library, trying precompiled first, then falling back to source.
-spec load_nif() -> ok | {error, term()}.
load_nif() ->
    load_nif([]).

%% @doc Load the NIF library with options.
%% Options:
%%   - force_download: boolean() - Force download even if local exists
%%   - force_compile: boolean() - Skip precompiled, compile from source
%%   - version: string() - Specific version to download
-spec load_nif(proplists:proplist()) -> ok | {error, term()}.
load_nif(Opts) ->
    ForceCompile = proplists:get_value(force_compile, Opts, false),
    case ForceCompile of
        true ->
            load_from_source();
        false ->
            try_load_precompiled(Opts)
    end.

%% @doc Get platform information for the current system.
-spec get_platform_info() -> platform_info().
get_platform_info() ->
    OS = get_os(),
    Arch = get_arch(),
    ABI = get_abi(OS),
    Target = build_target(OS, Arch, ABI),
    #{
        os => OS,
        arch => Arch,
        abi => ABI,
        target => Target
    }.

%% @doc Download precompiled NIF for the given version.
-spec download_precompiled(string()) -> {ok, file:filename()} | {error, term()}.
download_precompiled(Version) ->
    #{target := Target} = get_platform_info(),
    PrivDir = code:priv_dir(?APP_NAME),
    CrateDir = filename:join([PrivDir, "crates", "cozodb"]),
    ok = filelib:ensure_dir(filename:join(CrateDir, "dummy")),

    ArtifactName = build_artifact_name(Version, Target),
    Url = build_download_url(Version, ArtifactName),

    TmpDir = filename:join([PrivDir, "tmp"]),
    ok = filelib:ensure_dir(filename:join(TmpDir, "dummy")),
    TmpFile = filename:join(TmpDir, ArtifactName),

    logger:info("Downloading precompiled NIF from: ~s", [Url]),
    case download_file(Url, TmpFile) of
        ok ->
            extract_and_install(TmpFile, CrateDir, Version, Target);
        {error, _} = DownloadErr ->
            DownloadErr
    end.

%% @doc Verify a file's SHA256 checksum.
-spec verify_checksum(file:filename(), binary()) -> ok | {error, checksum_mismatch}.
verify_checksum(FilePath, ExpectedChecksum) ->
    case file:read_file(FilePath) of
        {ok, Content} ->
            ActualChecksum = crypto:hash(sha256, Content),
            ActualHex = bin_to_hex(ActualChecksum),
            case string:lowercase(binary_to_list(ExpectedChecksum)) =:=
                 string:lowercase(binary_to_list(ActualHex)) of
                true -> ok;
                false -> {error, checksum_mismatch}
            end;
        {error, Reason} ->
            {error, {file_read_error, Reason}}
    end.

%%====================================================================
%% Internal functions
%%====================================================================

try_load_precompiled(Opts) ->
    PrivDir = code:priv_dir(?APP_NAME),
    NifPath = filename:join([PrivDir, "crates", "cozodb", "cozodb"]),

    %% Check for NIF file with any extension (.so, .dylib, .dll)
    NifExists = filelib:is_file(NifPath ++ ".so") orelse
                filelib:is_file(NifPath ++ ".dylib") orelse
                filelib:is_file(NifPath ++ ".dll"),

    case NifExists of
        true ->
            ForceDownload = proplists:get_value(force_download, Opts, false),
            case ForceDownload of
                true ->
                    download_and_load(Opts);
                false ->
                    load_nif_file(NifPath)
            end;
        false ->
            download_and_load(Opts)
    end.

download_and_load(Opts) ->
    Version = proplists:get_value(version, Opts, get_app_version()),
    case download_precompiled(Version) of
        {ok, NifPath} ->
            load_nif_file(NifPath);
        {error, Reason} ->
            logger:warning("Failed to download precompiled NIF: ~p, falling back to source", [Reason]),
            load_from_source()
    end.

load_nif_file(Path) ->
    case erlang:load_nif(Path, 0) of
        ok -> ok;
        {error, {reload, _}} -> ok;
        {error, Reason} -> {error, {nif_load_error, Reason}}
    end.

load_from_source() ->
    PrivDir = code:priv_dir(?APP_NAME),
    NifPath = filename:join([PrivDir, "crates", "cozodb", "cozodb"]),
    load_nif_file(NifPath).

get_os() ->
    case os:type() of
        {unix, linux} -> linux;
        {unix, darwin} -> darwin;
        {win32, _} -> windows;
        _ -> unknown
    end.

get_arch() ->
    case erlang:system_info(system_architecture) of
        Arch when is_list(Arch) ->
            ArchLower = string:lowercase(Arch),
            case {string:find(ArchLower, "x86_64"), string:find(ArchLower, "aarch64"),
                  string:find(ArchLower, "arm"), string:find(ArchLower, "riscv64")} of
                {nomatch, nomatch, nomatch, nomatch} ->
                    case string:find(ArchLower, "x86") of
                        nomatch -> unknown;
                        _ -> x86_64
                    end;
                {_, nomatch, nomatch, nomatch} -> x86_64;
                {nomatch, _, nomatch, nomatch} -> aarch64;
                {nomatch, nomatch, _, nomatch} -> arm;
                {nomatch, nomatch, nomatch, _} -> riscv64
            end;
        _ ->
            unknown
    end.

get_abi(linux) ->
    case is_musl() of
        true -> musl;
        false ->
            case get_arch() of
                arm -> gnueabihf;
                _ -> gnu
            end
    end;
get_abi(darwin) ->
    darwin;
get_abi(windows) ->
    msvc;
get_abi(_) ->
    unknown.

is_musl() ->
    case os:cmd("ldd --version 2>&1") of
        Output ->
            string:find(string:lowercase(Output), "musl") =/= nomatch
    end.

build_target(darwin, Arch, _ABI) ->
    ArchStr = atom_to_list(Arch),
    iolist_to_binary([ArchStr, "-apple-darwin"]);
build_target(linux, Arch, musl) ->
    ArchStr = atom_to_list(Arch),
    iolist_to_binary([ArchStr, "-unknown-linux-musl"]);
build_target(linux, arm, gnueabihf) ->
    <<"arm-unknown-linux-gnueabihf">>;
build_target(linux, Arch, gnu) ->
    ArchStr = atom_to_list(Arch),
    iolist_to_binary([ArchStr, "-unknown-linux-gnu"]);
build_target(windows, Arch, msvc) ->
    ArchStr = atom_to_list(Arch),
    iolist_to_binary([ArchStr, "-pc-windows-msvc"]);
build_target(_, _, _) ->
    <<"unknown">>.

%% Build artifact name in rustler_precompiled format:
%% lib{crate}-v{version}-nif-{nif_version}-{target}.{ext}.tar.gz
build_artifact_name(Version, Target) ->
    Ext = get_lib_extension(Target),
    lists:flatten(io_lib:format(
        "lib~s-v~s-nif-~s-~s~s.tar.gz",
        [?CRATE_NAME, Version, ?NIF_VERSION, Target, Ext]
    )).

%% Build the download URL for GitHub releases
build_download_url(Version, ArtifactName) ->
    GithubRepo = get_github_repo(),
    Tag = "v" ++ Version,
    lists:flatten(io_lib:format(
        "https://github.com/~s/releases/download/~s/~s",
        [GithubRepo, Tag, ArtifactName]
    )).

%% Get the library file extension for the target
get_lib_extension(Target) ->
    TargetStr = binary_to_list(Target),
    case string:find(TargetStr, "windows") of
        nomatch ->
            %% Both Linux and macOS use .so for Erlang NIFs
            ".so";
        _ ->
            ".dll"
    end.

%% Get GitHub repo from environment or default
get_github_repo() ->
    case os:getenv("COZODB_GITHUB_REPO") of
        false -> ?DEFAULT_GITHUB_REPO;
        Repo -> Repo
    end.

download_file(Url, DestPath) ->
    ensure_http_client(),
    case httpc:request(get, {Url, []}, [{timeout, 120000}], [{stream, DestPath}]) of
        {ok, saved_to_file} ->
            ok;
        {ok, {{_, 200, _}, _, Body}} ->
            file:write_file(DestPath, Body);
        {ok, {{_, 404, _}, _, _}} ->
            {error, not_found};
        {ok, {{_, Status, _}, _, _}} ->
            {error, {http_error, Status}};
        {error, Reason} ->
            {error, {download_failed, Reason}}
    end.

download_and_verify_checksum(ChecksumUrl, FilePath) ->
    TmpChecksum = FilePath ++ ".sha256.tmp",
    case download_file(ChecksumUrl, TmpChecksum) of
        ok ->
            case file:read_file(TmpChecksum) of
                {ok, ChecksumContent} ->
                    file:delete(TmpChecksum),
                    [ExpectedChecksum | _] = binary:split(ChecksumContent, [<<" ">>, <<"\t">>, <<"\n">>]),
                    verify_checksum(FilePath, ExpectedChecksum);
                {error, Reason} ->
                    file:delete(TmpChecksum),
                    {error, {checksum_read_error, Reason}}
            end;
        {error, not_found} ->
            logger:warning("Checksum file not found, skipping verification"),
            ok;
        {error, Reason} ->
            {error, {checksum_download_failed, Reason}}
    end.

%% Extract tar.gz and rename the NIF file to standard name
extract_and_install(TarFile, DestDir, Version, Target) ->
    case erl_tar:extract(TarFile, [{cwd, DestDir}, compressed]) of
        ok ->
            file:delete(TarFile),
            %% The extracted file has rustler_precompiled naming:
            %% lib{crate}-v{version}-nif-{nif_version}-{target}.{ext}
            Ext = get_lib_extension(Target),
            ExtractedName = lists:flatten(io_lib:format(
                "lib~s-v~s-nif-~s-~s~s",
                [?CRATE_NAME, Version, ?NIF_VERSION, Target, Ext]
            )),
            ExtractedPath = filename:join(DestDir, ExtractedName),
            %% Rename to standard name without lib prefix and version
            FinalExt = case Ext of
                ".dll" -> ".dll";
                _ -> ".so"
            end,
            FinalPath = filename:join(DestDir, "cozodb" ++ FinalExt),
            case file:rename(ExtractedPath, FinalPath) of
                ok ->
                    NifPath = filename:join(DestDir, "cozodb"),
                    {ok, NifPath};
                {error, RenameReason} ->
                    logger:warning("Failed to rename ~s to ~s: ~p",
                                   [ExtractedPath, FinalPath, RenameReason]),
                    %% Try to use the extracted file directly
                    NifPath = filename:rootname(ExtractedPath),
                    {ok, NifPath}
            end;
        {error, Reason} ->
            file:delete(TarFile),
            {error, {extract_failed, Reason}}
    end.

ensure_http_client() ->
    case application:ensure_all_started(inets) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok
    end,
    case application:ensure_all_started(ssl) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok
    end.

get_app_version() ->
    case application:get_key(?APP_NAME, vsn) of
        {ok, Vsn} -> Vsn;
        undefined -> "0.0.0"
    end.

bin_to_hex(Bin) ->
    << <<(hex_char(H)), (hex_char(L))>> || <<H:4, L:4>> <= Bin >>.

hex_char(N) when N < 10 -> $0 + N;
hex_char(N) -> $a + N - 10.
