#!/bin/sh
# Build and run the local executable.
set -eu
project_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
(cd "$project_dir" && zig build)
exec "$project_dir/zig-out/bin/main" "$@"
