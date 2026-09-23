#!/usr/bin/env bash
# Copyright 2026 The PyAthena authors
#
# Licensed under the MIT License.
# See LICENSE or https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: MIT

set -euo pipefail

worktree_root=$(git rev-parse --show-toplevel)
common_dir=$(git rev-parse --path-format=absolute --git-common-dir)
main_root=$(cd "$(dirname "$common_dir")" && pwd -P)

cd "$worktree_root"

if [ "$(pwd -P)" = "$main_root" ]; then
    echo "This is the main checkout; nothing to link."
    exit 0
fi

if [ -e .env ] && [ ! -L .env ]; then
    echo ".env is already a regular file in this worktree; nothing to link."
    exit 0
fi

if [ ! -f "$main_root/.env" ]; then
    echo "error: $main_root/.env does not exist" >&2
    exit 1
fi

ln -sfn "$main_root/.env" .env
echo "Linked .env -> $main_root/.env"
