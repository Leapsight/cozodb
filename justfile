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
