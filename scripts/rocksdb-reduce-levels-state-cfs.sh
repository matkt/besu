#!/usr/bin/env bash
# Compact (and optionally reduce_levels) Besu state CFs: 0x06, 0x08, 0x09 (TAB).
#
# Besu must be stopped. Prefer a database COPY.
#
# Note: ldb reduce_levels only looks at the *default* CF to decide whether to run.
# Besu's default CF is usually empty, so reduce_levels often no-ops. This script
# always runs `compact` on account / storage / trie; that is what actually moves data.
# Use Besu --Xplugin-rocksdb-num-levels=2 after compaction for new writes.
#
# Usage:
#   export LDB=/usr/local/bin/ldb-besu
#   export ROCKSDB_LD_LIBRARY_PATH=/tmp/rocksdb
#   sudo env LDB="$LDB" ROCKSDB_LD_LIBRARY_PATH="$ROCKSDB_LD_LIBRARY_PATH" \
#     ./rocksdb-reduce-levels-state-cfs.sh /data/besu/database-numlevels-test 2
#
# ldb must be built WITH LZ4 (Besu CFs use LZ4):
#   sudo dnf install -y lz4-devel   # or apt install liblz4-dev
#   cd /tmp/rocksdb && make clean
#   DEBUG_LEVEL=0 USE_LZ4=1 make -j$(nproc) ldb shared_lib
#
set -euo pipefail

DB="${1:?database path}"
NEW_LEVELS="${2:-2}"
LDB="${LDB:-ldb}"
RUN_REDUCE_LEVELS="${RUN_REDUCE_LEVELS:-0}"

if [[ ! -d "$DB" ]]; then
  echo "error: not a directory: $DB" >&2
  exit 1
fi

if [[ ! -x "$LDB" ]] && ! command -v "$LDB" >/dev/null 2>&1; then
  echo "error: ldb not found or not executable: $LDB" >&2
  exit 1
fi

run_ldb() {
  if [[ -n "${ROCKSDB_LD_LIBRARY_PATH:-}" ]]; then
    LD_LIBRARY_PATH="${ROCKSDB_LD_LIBRARY_PATH}${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}" \
      "$LDB" "$@"
  else
    "$LDB" "$@"
  fi
}

# ldb without a subcommand prints help and exits non-zero — do not treat that as failure.
ldb_has_command() {
  local cmd="$1"
  local help_text
  help_text="$(run_ldb 2>&1 || true)"
  grep -qE "(^|[[:space:]])${cmd}([[:space:]]|$|--)" <<<"$help_text"
}

if ! ldb_has_command compact; then
  echo "error: $LDB does not list 'compact'. Use RocksDB 10.6.x ldb." >&2
  exit 1
fi

if [[ "$RUN_REDUCE_LEVELS" == "1" ]] && ! ldb_has_command reduce_levels; then
  echo "error: RUN_REDUCE_LEVELS=1 but reduce_levels not in $LDB" >&2
  exit 1
fi

if [[ -z "${ROCKSDB_LD_LIBRARY_PATH:-}" ]]; then
  if LD_LIBRARY_PATH="" ldd "$LDB" 2>/dev/null | grep -q 'not found'; then
    echo "warning: set ROCKSDB_LD_LIBRARY_PATH to your rocksdb build dir (librocksdb*.so)" >&2
  fi
fi

CF_SPECS=(
  "ACCOUNT_INFO_STATE|06|$(printf '\006')"
  "ACCOUNT_STORAGE_STORAGE|08|$(printf '\010')"
  "TRIE_BRANCH_STORAGE|09|$(printf '\t')"
)

common_flags=(--db="$DB" --ignore_unknown_options)

echo "Database: $DB"
echo "ldb: $LDB"
echo "new_levels (Besu CLI / optional reduce_levels): $NEW_LEVELS"
echo "RUN_REDUCE_LEVELS: $RUN_REDUCE_LEVELS"
[[ -n "${ROCKSDB_LD_LIBRARY_PATH:-}" ]] && echo "ROCKSDB_LD_LIBRARY_PATH: $ROCKSDB_LD_LIBRARY_PATH"
echo

for spec in "${CF_SPECS[@]}"; do
  IFS='|' read -r name hex cf <<<"$spec"
  echo "=== $name (0x$hex) — compact ==="
  if ! run_ldb "${common_flags[@]}" --column_family="$cf" compact; then
    echo "error: compact failed for $name (0x$hex)" >&2
    exit 1
  fi
  echo "levelstats:"
  run_ldb "${common_flags[@]}" --column_family="$cf" get_property rocksdb.levelstats \
    | head -20 || true
  echo
done

if [[ "$RUN_REDUCE_LEVELS" == "1" ]]; then
  echo "=== reduce_levels (global ldb quirk: may no-op if default CF is empty) ==="
  out="$(run_ldb "${common_flags[@]}" reduce_levels --new_levels="$NEW_LEVELS" --print_old_levels 2>&1)" || {
    echo "$out"
    echo "warning: reduce_levels failed (see above)" >&2
  }
  echo "$out"
  if ! grep -q "Compacting the db" <<<"$out"; then
    echo
    echo "note: reduce_levels did not compact (Besu default CF often has 0 levels in use)."
    echo "      Compaction above on 0x06/0x08/0x09 is the useful part."
    echo "      Start Besu with --Xplugin-rocksdb-num-levels=$NEW_LEVELS on this DB copy."
  fi
fi

echo
echo "Done. Verify: python3 rocksdb-besu-lsm-report.py $DB"
