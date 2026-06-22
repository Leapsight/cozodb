# cozodb tasks. Run `just` to list recipes.

_default:
    @just --list

# Run the archiving/replication integration tests against S3 (loads ./.env if present).
test-archive:
    #!/usr/bin/env bash
    set -euo pipefail
    # Loads AWS_* + COZO_TEST_S3_BUCKET; the suite skips cleanly when unset.
    if [ -f .env ]; then set -a; . ./.env; set +a; fi
    rebar3 ct --suite test/cozodb_archive_SUITE

# Run the full Common Test suite (loads ./.env so the archiving suite runs).
test:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ -f .env ]; then set -a; . ./.env; set +a; fi
    rebar3 ct

# Run a single CT suite by name, e.g. `just ct cozodb_archive_SUITE`.
ct suite:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ -f .env ]; then set -a; . ./.env; set +a; fi
    rebar3 ct --suite test/{{suite}}

# Run EUnit tests.
eunit:
    rebar3 eunit

# Print the current library version.
version:
    @perl -ne 'print "$1\n" if /\{vsn,\s*"([^"]+)"\}/' src/cozodb.app.src

# Set the library version everywhere (OTP app vsn + NIF crate + relx release).
set-version new:
    #!/usr/bin/env bash
    # Updates src/cozodb.app.src, native/cozodb/Cargo.toml and the rebar.config
    # relx release, keyed on the current vsn so third-party dependency versions
    # are left untouched.  Usage: just set-version 0.3.11
    set -euo pipefail
    new="{{new}}"
    if ! [[ "$new" =~ ^[0-9]+\.[0-9]+\.[0-9]+([.\-+].*)?$ ]]; then
        echo "error: '$new' does not look like a version (e.g. 0.3.11)" >&2
        exit 1
    fi
    old="$(perl -ne 'print $1 if /\{vsn,\s*"([^"]+)"\}/' src/cozodb.app.src)"
    if [[ -z "$old" ]]; then
        echo "error: could not read current vsn from src/cozodb.app.src" >&2
        exit 1
    fi
    if [[ "$old" == "$new" ]]; then
        echo "already at $new; nothing to do"
        exit 0
    fi
    echo "Bumping $old -> $new"
    # OTP application version (canonical — hex + git tag track this).
    old="$old" new="$new" perl -i -pe 's/\{vsn,\s*"\Q$ENV{old}\E"\}/{vsn, "$ENV{new}"}/' src/cozodb.app.src
    echo "  src/cozodb.app.src"
    # NIF crate version (exact-match keyed so dep `version = "..."` lines are safe).
    old="$old" new="$new" perl -i -pe 's/^version = "\Q$ENV{old}\E"/version = "$ENV{new}"/' native/cozodb/Cargo.toml
    echo "  native/cozodb/Cargo.toml"
    # relx release version (kept in lockstep with the library).
    new="$new" perl -i -pe 's/\{cozodb,\s*"[^"]*"\}/{cozodb, "$ENV{new}"}/' rebar.config
    echo "  rebar.config (relx release)"
    echo "Done. Review with: git diff"

# Create the git tag for the current library version on HEAD.
# Tags are bare (e.g. `0.3.11`). Pass force=true to move an existing tag.
tag force="false":
    #!/usr/bin/env bash
    set -euo pipefail
    ver="$(perl -ne 'print $1 if /\{vsn,\s*"([^"]+)"\}/' src/cozodb.app.src)"
    if [[ "{{force}}" == "true" ]]; then
        git tag -f "$ver"
        echo "moved tag $ver -> $(git rev-parse --short HEAD)"
    else
        git tag "$ver"
        echo "created tag $ver -> $(git rev-parse --short HEAD)"
    fi
    echo "push it with: git push origin $ver"
