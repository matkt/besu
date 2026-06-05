#!/usr/bin/env bash
# Read-only PlainTable RocksDB stats for frozen snap-sync trie nodes (no Besu rebuild).
#
# Usage:
#   ./scripts/frozen-snap-trie-map-stats/stats.sh /path/to/data/frozen_snap_trie_nodes
#   BESU_LIB=/path/to/besu/lib ./scripts/frozen-snap-trie-map-stats/stats.sh ...
#
# Requires: JDK 21+, Besu libs once (installDist) OR BESU_LIB pointing at a besu install lib/
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BUILD_DIR="$SCRIPT_DIR/.build"
MAIN_CLASS="FrozenSnapTrieNodeMapStats"
SOURCE="$SCRIPT_DIR/$MAIN_CLASS.java"

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <frozen_snap_trie_nodes-dir>" >&2
  exit 2
fi

MAP_ARG="$1"

if [[ -z "${JAVA_HOME:-}" ]] && [[ -x "$HOME/.jdks/jdk-25.0.3+9/Contents/Home/bin/java" ]]; then
  export JAVA_HOME="$HOME/.jdks/jdk-25.0.3+9/Contents/Home"
fi
JAVA="${JAVA_HOME:-}/bin/java"
JAVAC="${JAVA_HOME:-}/bin/javac"
if [[ ! -x "$JAVA" ]]; then
  JAVA="$(command -v java)"
fi
if [[ ! -x "$JAVAC" ]]; then
  JAVAC="$(command -v javac)"
fi

resolve_besu_lib() {
  if [[ -n "${BESU_LIB:-}" ]]; then
    echo "$BESU_LIB"
    return
  fi
  local install_lib="$REPO_ROOT/build/install/besu/lib"
  if [[ -d "$install_lib" ]]; then
    echo "$install_lib"
    return
  fi
  echo ""
}

BESU_LIB_DIR="$(resolve_besu_lib)"
if [[ -z "$BESU_LIB_DIR" || ! -d "$BESU_LIB_DIR" ]]; then
  cat >&2 <<EOF
No Besu lib directory found. Either:
  1) Run once: ./gradlew installDist   (then re-use build/install/besu/lib)
  2) Or set:   export BESU_LIB=/path/to/besu/lib
EOF
  exit 1
fi

ROCKSDB_JAR="$(find "$BESU_LIB_DIR" -maxdepth 1 -name 'rocksdbjni-*.jar' -print -quit)"
if [[ -z "$ROCKSDB_JAR" ]]; then
  echo "rocksdbjni jar not found under $BESU_LIB_DIR — installDist from this branch required." >&2
  exit 1
fi

CLASSPATH="$BUILD_DIR"
while IFS= read -r -d '' jar; do
  CLASSPATH="${CLASSPATH}:${jar}"
done < <(find "$BESU_LIB_DIR" -maxdepth 1 -name '*.jar' -print0)

mkdir -p "$BUILD_DIR"
CLASS_FILE="$BUILD_DIR/${MAIN_CLASS}.class"
if [[ ! -f "$CLASS_FILE" ]] || [[ "$SOURCE" -nt "$CLASS_FILE" ]]; then
  echo "Compiling $MAIN_CLASS (one-time) ..." >&2
  "$JAVA" -version >&2
  "$JAVAC" -cp "$CLASSPATH" -d "$BUILD_DIR" "$SOURCE"
fi

JVM_OPTS=(
  "--enable-native-access=ALL-UNNAMED"
  "--add-opens=java.base/java.nio=ALL-UNNAMED"
  "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED"
)

exec "$JAVA" "${JVM_OPTS[@]}" -cp "$CLASSPATH" "$MAIN_CLASS" "$MAP_ARG"
