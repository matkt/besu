#!/usr/bin/env bash
# Copyright contributors to Hyperledger Besu.
# SPDX-License-Identifier: Apache-2.0
#
# Print entry count and on-disk size for snap_trie_nodes.dat (read-only).
#
# Usage:
#   ./scripts/frozen-snap-trie-map-stats.sh /path/to/besu-data-dir
#   ./scripts/frozen-snap-trie-map-stats.sh /path/to/besu-data-dir/frozen_snap_trie_nodes

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <besu-data-dir|frozen_snap_trie_nodes-dir>" >&2
  exit 2
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MAP_PATH="$1"

if [[ -f "${ROOT}/.java25.env" ]]; then
  # shellcheck source=/dev/null
  source "${ROOT}/.java25.env"
fi

cd "${ROOT}"
./gradlew :services:kvstore:frozenSnapTrieMapStats -PmapPath="${MAP_PATH}" -q
