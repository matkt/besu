#!/usr/bin/env bash
#
# Glamsterdam devnet 6 — Besu (EL) + Prysm (CL) Docker setup
#
# Connects a local Besu execution client and Prysm beacon node to the
# ethPandaOps Glamsterdam devnet 6 testnet.
#
# Network config: https://glamsterdam-devnet-6.ethpandaops.io/
#
# Usage examples:
#   # Start with Besu built from the current repo branch (default)
#   ./setup.sh start
#
#   # Custom Besu data directory (required mount path on host)
#   ./setup.sh start --data-dir /path/to/besu-data
#
#   # Force rebuild Besu from current branch before starting
#   ./setup.sh start --build-besu --data-dir /tmp/besu
#
#   # Build Besu image only (no containers)
#   ./setup.sh build
#
#   # Use ethPandaOps pre-built Besu instead of local branch build
#   ./setup.sh start --besu-image ethpandaops/besu:glamsterdam-devnet-6
#
#   # Custom Prysm image (Besu image is also configurable)
#   ./setup.sh start --prysm-image ethpandaops/prysm-beacon-chain:glamsterdam-devnet-6
#
#   # Follow Besu block import logs
#   ./setup.sh logs besu
#
#   # Follow only BAL/nonce/cache diagnostic lines (visible at INFO)
#   ./setup.sh logs-diag besu
#
#   # Stop and remove containers (data dirs are preserved)
#   ./setup.sh stop
#
#   # Restart Prysm only (Besu keeps running and syncing)
#   ./setup.sh restart-prysm
#
#   # Wipe Prysm data and restart checkpoint sync (Besu untouched; requires --confirm)
#   ./setup.sh resync-prysm --confirm
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# --- Glamsterdam devnet 6 network constants (ethPandaOps) ---
readonly CONFIG_BASE_URL="https://config.glamsterdam-devnet-6.ethpandaops.io"
readonly INVENTORY_URL="${CONFIG_BASE_URL}/api/v1/nodes/inventory"
readonly CHAIN_ID="7052886157"
readonly DEPOSIT_CONTRACT="0x00000000219ab540356cBB839Cbe05303d7705Fa"
readonly DEFAULT_CHECKPOINT_SYNC_URL="https://checkpoint-sync.glamsterdam-devnet-6.ethpandaops.io"
# Default EL bootnodes (Glamsterdam devnet 6); override with BESU_BOOTNODES or --bootnodes.
readonly DEFAULT_BESU_BOOTNODES="enode://81a9bfc4bf1b90281851efda86d505edafb7f11428d3bfeca80d299024e20d1e7f0b027536e206fc8057ae0f96898a9c4eeffd51c0f41ca3d74f7fbb928024a2@137.184.216.66:9010,enode://34e356ff666f501f22fec4eddbdbc4960d15ebe25099f5075420a53282380dbbf5d984fb57932b2fc02bf55c430fb96637cc8624eb6aca754048dd703bb0ed39@137.184.216.66:30303?discport=30303,enode://3c1c84648c20cd0dc13e18c3092adebe625488979be9c87e8720c3025299f9e65a5331230b9252a74e19d59eccf24ac9d6ae48b3c5e4f95f47c9ed2fe5ae2bd0@143.198.181.19:30303?discport=30303,enode://30f01e05030bfb971ab7f1c4ce039f5a066dae7d16f5fb1a460f29ce5e92685601bf165043445bdadd65893735be0d4524dac5f52ccb770017c4ea3116e5cf86@206.189.200.7:30303?discport=30303,enode://f91480a7bd859d15e761c7e2a33ca7008da079161080cc42b7b6b95f45c79b5b9221e1bca271ee8cb01ef65a40be2c70f16903b9daef8e5833c20911fb1f3450@142.93.113.46:30303?discport=30303,enode://7c814b898ae16097ac276933550070ed2883785843e68471f1dc9c204c03f2002e92ba270333df81235276169fdd4f528caf746037d4c619a42b1c2db227c7fb@68.183.111.159:30303?discport=30303"

# --- Defaults (overridable via CLI) ---
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DEFAULT_DATA_DIR="${HOME}/.glamsterdam-devnet6"
# Built from current repo branch via `./setup.sh build` or auto-built on first `start`.
DEFAULT_BESU_IMAGE="besu:glamsterdam-devnet6-local"
DEFAULT_PRYSM_IMAGE="ethpandaops/prysm-beacon-chain:glamsterdam-devnet-6"

DATA_DIR=""
BESU_DATA_DIR=""
PRYSM_DATA_DIR=""
CONFIG_DIR=""
JWT_DIR=""
BESU_IMAGE="${DEFAULT_BESU_IMAGE}"
PRYSM_IMAGE="${DEFAULT_PRYSM_IMAGE}"
CHECKPOINT_SYNC_URL="${DEFAULT_CHECKPOINT_SYNC_URL}"
BESU_BOOTNODES="${BESU_BOOTNODES:-}"
P2P_HOST="${P2P_HOST:-0.0.0.0}"
EXTRA_STATIC_PEERS="${EXTRA_STATIC_PEERS:-}"
BESU_MAX_PEERS="${BESU_MAX_PEERS:-}"
BESU_USE_STATIC_NODES="${BESU_USE_STATIC_NODES:-false}"
FETCH_EL_BOOTNODES="${FETCH_EL_BOOTNODES:-false}"
REFRESH_CONFIG=false
BUILD_BESU=false
SKIP_BUILD_BESU=false
DETACH=true
LOG_SERVICE=""
PRYSM_ONLY=false
RESYNC_PRYSM_CONFIRM=false

usage() {
  sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'
  cat <<'EOF'

Commands:
  start                 Download config (if needed) and start Besu + Prysm
  build                 Build Besu Docker image from the current repo branch
  stop                  Stop and remove containers (keeps data)
  restart               stop then start (or restart Prysm only with --prysm-only)
  restart-prysm         Restart Prysm container only (Besu keeps running)
  resync-prysm          Wipe Prysm datadir and restart Prysm only (fresh checkpoint sync)
  logs [service]        Follow logs (default: all; use 'besu' for EL block imports)
  logs-diag [service]   Follow diagnostic lines only (default: besu; grep [BAL-DIAG])
  status                Show container status
  config                Download/refresh network config only
  validate              Run 'docker compose config' to validate compose file

Options:
  -d, --data-dir PATH       Base directory for all node data (default: ~/.glamsterdam-devnet6)
      --besu-data-dir PATH  Besu data path override (default: <data-dir>/besu)
      --prysm-image IMAGE   Prysm beacon-chain Docker image (default: ethpandaops/prysm-beacon-chain:glamsterdam-devnet-6)
      --besu-image IMAGE    Besu Docker image (default: besu:glamsterdam-devnet6-local, built from repo)
      --build-besu          Build/rebuild Besu from current branch before start (or with build command)
      --no-build-besu       Skip auto-build when local Besu image is missing (fail instead)
      --checkpoint-sync-url URL  Prysm checkpoint sync beacon API URL
      --p2p-host HOST         Besu --p2p-host (default: 0.0.0.0; use public IP for routable nodes)
      --bootnodes ENODES      Override EL bootnodes (comma-separated; default: Glamsterdam devnet 6 list)
      --fetch-el-bootnodes    Fetch EL bootnodes from ethPandaOps inventory instead of default list
      --use-static-nodes      Enable Besu --static-nodes-file=/config/static-nodes.json
      --static-peers ENODES   Extra EL enode URLs (comma-separated) added to static-nodes.json
      --max-peers N           Besu --max-peers (optional; no limit if omitted)
      --refresh-config        Re-download genesis/config from ethPandaOps
      --foreground            Run docker compose in foreground (no -d)
      --prysm-only            With restart: restart Prysm only (Besu untouched)
      --confirm               Required with resync-prysm to wipe Prysm data
  -h, --help                Show this help

Environment:
  BESU_DATA_DIR, PRYSM_IMAGE, BESU_IMAGE, DATA_DIR, P2P_HOST, BESU_BOOTNODES,
  BESU_USE_STATIC_NODES, FETCH_EL_BOOTNODES, EXTRA_STATIC_PEERS, BESU_MAX_PEERS
  can also be set in the environment.
EOF
}

log() { printf '[glamsterdam6] %s\n' "$*"; }
die() { log "ERROR: $*" >&2; exit 1; }

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Required command not found: $1"
}

load_env_file() {
  local env_file="${SCRIPT_DIR}/.env"
  [[ -f "${env_file}" ]] || return 1
  while IFS='=' read -r key value; do
    [[ "${key}" =~ ^#.*$ || -z "${key}" ]] && continue
    case "${key}" in
      BESU_DATA_DIR|PRYSM_DATA_DIR|CONFIG_DIR|JWT_DIR|BESU_IMAGE|PRYSM_IMAGE)
        printf -v "${key}" '%s' "${value}"
        ;;
    esac
  done < "${env_file}"
  return 0
}

resolve_paths() {
  DATA_DIR="${DATA_DIR:-${DEFAULT_DATA_DIR}}"
  BESU_DATA_DIR="${BESU_DATA_DIR:-${DATA_DIR}/besu}"
  PRYSM_DATA_DIR="${PRYSM_DATA_DIR:-${DATA_DIR}/prysm}"
  CONFIG_DIR="${CONFIG_DIR:-${DATA_DIR}/config}"
  JWT_DIR="${JWT_DIR:-${DATA_DIR}/jwt}"
}

preserve_paths_from_env_file() {
  [[ -n "${DATA_DIR}" ]] && return 0
  load_env_file || return 0
  if [[ -n "${BESU_DATA_DIR}" ]]; then
    DATA_DIR="$(dirname "${BESU_DATA_DIR}")"
  fi
}

download_file() {
  local url="$1" dest="$2"
  log "Downloading $(basename "$dest") ..."
  curl -fsSL "$url" -o "$dest"
}

resolve_besu_bootnodes() {
  if [[ "${FETCH_EL_BOOTNODES}" == "true" ]]; then
    require_cmd jq
    log "Fetching EL bootnodes from ethPandaOps inventory ..."
    local inventory
    inventory="$(curl -fsSL "${INVENTORY_URL}")"
    BESU_BOOTNODES="$(echo "$inventory" | jq -r '[.ethereum_pairs[].execution.enode] | unique | .[:5] | join(",")')"
    [[ -n "${BESU_BOOTNODES}" ]] || die "Failed to fetch EL bootnodes from ${INVENTORY_URL}"
  else
    BESU_BOOTNODES="${BESU_BOOTNODES:-${DEFAULT_BESU_BOOTNODES}}"
  fi
}

fetch_el_peers() {
  require_cmd jq
  log "Fetching CL peers from ethPandaOps inventory ..."
  local inventory static_nodes_json
  inventory="$(curl -fsSL "${INVENTORY_URL}")"

  resolve_besu_bootnodes

  # Up to 5 unique CL ENRs to keep command lines manageable.
  PRYSM_BOOTNODES="$(echo "$inventory" | jq -r '[.ethereum_pairs[].consensus.enr] | unique | .[:5] | join(",")')"

  [[ -n "${PRYSM_BOOTNODES}" ]] || die "Failed to fetch CL bootnodes from ${INVENTORY_URL}"

  # All unique EL enodes as static peers (maintains persistent outbound connections).
  static_nodes_json="$(echo "$inventory" | jq -c '[.ethereum_pairs[].execution.enode] | unique')"
  if [[ -n "${EXTRA_STATIC_PEERS}" ]]; then
    static_nodes_json="$(echo "$static_nodes_json" | jq -c --arg extras "${EXTRA_STATIC_PEERS}" '
      . + ($extras | split(",") | map(gsub("^\\s+|\\s+$"; "")) | map(select(length > 0))) | unique
    ')"
  fi

  printf '%s\n' "${BESU_BOOTNODES}" > "${CONFIG_DIR}/besu-bootnodes.txt"
  # Prysm rejects comma-separated ENRs in one --bootstrap-node value; use a YAML list file instead.
  {
    echo "# Auto-generated by setup.sh — one ENR per list entry for Prysm --bootstrap-node"
    IFS=',' read -ra prysm_enrs <<< "${PRYSM_BOOTNODES}"
    for enr in "${prysm_enrs[@]}"; do
      enr="${enr#"${enr%%[![:space:]]*}"}"
      enr="${enr%"${enr##*[![:space:]]}"}"
      [[ -n "${enr}" ]] && printf -- '- %s\n' "${enr}"
    done
  } > "${CONFIG_DIR}/prysm-bootnodes.yaml"
  printf '%s\n' "${static_nodes_json}" > "${CONFIG_DIR}/static-nodes.json"

  local static_count
  static_count="$(echo "$static_nodes_json" | jq 'length')"
  log "Wrote ${static_count} EL static peer(s) to static-nodes.json"
}

validate_jwt_file() {
  local jwt_file="$1"
  [[ -f "${jwt_file}" ]] || die "JWT secret not found: ${jwt_file}"
  local jwt_len
  jwt_len="$(wc -c < "${jwt_file}" | tr -d ' ')"
  [[ "${jwt_len}" -eq 64 ]] || die "JWT secret must be 64 hex chars (32 bytes), got ${jwt_len} in ${jwt_file}"
  if ! grep -qE '^[0-9a-fA-F]{64}$' "${jwt_file}"; then
    die "JWT secret must be lowercase/uppercase hex only (no newlines) in ${jwt_file}"
  fi
}

ensure_jwt() {
  local jwt_file="${JWT_DIR}/execution-auth.jwt"
  local legacy_jwt_file="${JWT_DIR}/jwt.hex"
  mkdir -p "${JWT_DIR}"
  if [[ -f "${jwt_file}" ]]; then
    validate_jwt_file "${jwt_file}"
    return
  fi
  if [[ -f "${legacy_jwt_file}" ]]; then
    log "Migrating legacy jwt.hex to execution-auth.jwt ..."
    cp "${legacy_jwt_file}" "${jwt_file}"
    chmod 600 "${jwt_file}"
    validate_jwt_file "${jwt_file}"
    return
  fi
  log "Generating Engine API JWT secret ..."
  openssl rand -hex 32 | tr -d '\n' > "${jwt_file}"
  chmod 600 "${jwt_file}"
  validate_jwt_file "${jwt_file}"
}

sync_compose_path_env() {
  export JWT_DIR CONFIG_DIR BESU_DATA_DIR PRYSM_DATA_DIR
}

besu_container_jwt_source() {
  docker inspect glamsterdam6-besu \
    --format '{{range .Mounts}}{{if eq .Destination "/execution-auth.jwt"}}{{.Source}}{{end}}{{end}}' \
    2>/dev/null || true
}

verify_besu_jwt_mount() {
  local expected_jwt="${JWT_DIR}/execution-auth.jwt"
  local besu_jwt_source
  besu_jwt_source="$(besu_container_jwt_source)"
  [[ -n "${besu_jwt_source}" ]] || return 0
  if [[ "${besu_jwt_source}" != "${expected_jwt}" ]]; then
    die "JWT path mismatch: running Besu mounts ${besu_jwt_source} but .env JWT_DIR is ${JWT_DIR}. Restart both with the same --data-dir, or run: ./setup.sh stop && ./setup.sh start --data-dir $(dirname "$(dirname "${besu_jwt_source}")")"
  fi
  validate_jwt_file "${expected_jwt}"
}

download_network_config() {
  mkdir -p "${CONFIG_DIR}" "${BESU_DATA_DIR}" "${PRYSM_DATA_DIR}"

  local besu_genesis="${CONFIG_DIR}/besu.json"
  local cl_config="${CONFIG_DIR}/config.yaml"
  local cl_genesis="${CONFIG_DIR}/genesis.ssz"

  if [[ "${REFRESH_CONFIG}" == "true" || ! -f "${besu_genesis}" ]]; then
    download_file "${CONFIG_BASE_URL}/el/besu.json" "${besu_genesis}"
  fi
  if [[ "${REFRESH_CONFIG}" == "true" || ! -f "${cl_config}" ]]; then
    download_file "${CONFIG_BASE_URL}/cl/config.yaml" "${cl_config}"
  fi
  if [[ "${REFRESH_CONFIG}" == "true" || ! -f "${cl_genesis}" ]]; then
    download_file "${CONFIG_BASE_URL}/cl/genesis.ssz" "${cl_genesis}"
  fi

  fetch_el_peers
}

write_env_file() {
  local env_file="${SCRIPT_DIR}/.env"
  cat > "${env_file}" <<EOF
# Auto-generated by setup.sh — do not edit manually; re-run setup.sh to regenerate.
CHAIN_ID=${CHAIN_ID}
BESU_IMAGE=${BESU_IMAGE}
PRYSM_IMAGE=${PRYSM_IMAGE}
BESU_DATA_DIR=${BESU_DATA_DIR}
PRYSM_DATA_DIR=${PRYSM_DATA_DIR}
CONFIG_DIR=${CONFIG_DIR}
JWT_DIR=${JWT_DIR}
P2P_HOST=${P2P_HOST}
BESU_BOOTNODES=${BESU_BOOTNODES}
PRYSM_BOOTNODES=${PRYSM_BOOTNODES}
PRYSM_CHECKPOINT_SYNC_URL=${CHECKPOINT_SYNC_URL}
BESU_USE_STATIC_NODES=${BESU_USE_STATIC_NODES}
BESU_LOG_LEVEL=INFO
PRYSM_LOG_LEVEL=info
PRYSM_MIN_SYNC_PEERS=1
EOF
  if [[ -n "${BESU_MAX_PEERS}" ]]; then
    printf 'BESU_MAX_PEERS=%s\n' "${BESU_MAX_PEERS}" >> "${env_file}"
  fi
  log "Wrote ${env_file}"
}

validate_compose() {
  require_cmd docker
  local rendered
  rendered="$(cd "${SCRIPT_DIR}" && docker compose config)"
  if grep -qE -- '--max-peers=""|--p2p-peer-upper-bound=""' <<< "${rendered}"; then
    die "docker compose config passes empty --max-peers (set BESU_MAX_PEERS to an integer or omit it)"
  fi
  log "docker compose config: OK"
}

is_local_besu_image() {
  [[ "${BESU_IMAGE}" == "${DEFAULT_BESU_IMAGE}" ]]
}

besu_image_exists() {
  docker image inspect "${BESU_IMAGE}" >/dev/null 2>&1
}

besu_project_version() {
  local version=""
  version="$(
    cd "${REPO_ROOT}" && ./gradlew -q properties 2>/dev/null | awk -F': ' '/^version:/ {print $2; exit}'
  )" || true
  printf '%s' "${version}"
}

build_besu_image() {
  require_cmd docker
  [[ -f "${REPO_ROOT}/gradlew" ]] || die "Besu repo root not found at ${REPO_ROOT} (expected gradlew)"
  [[ -f "${REPO_ROOT}/docker/Dockerfile" ]] || die "Dockerfile not found at ${REPO_ROOT}/docker/Dockerfile"

  local version git_hash build_date docker_context
  version="$(besu_project_version)"
  [[ -n "${version}" ]] || version="glamsterdam-devnet6-local"
  git_hash="$(git -C "${REPO_ROOT}" rev-parse --short=7 HEAD 2>/dev/null || echo "unknown")"
  build_date="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  docker_context="${REPO_ROOT}/build/docker-besu"

  log "Building Besu Docker image from repo: ${REPO_ROOT}"
  log "Git commit: ${git_hash}  version: ${version}  tag: ${BESU_IMAGE}"
  if ! (
    cd "${REPO_ROOT}"
    ./gradlew --no-daemon distDockerCopy
  ); then
    die "Gradle distDockerCopy failed — check JAVA_HOME and run './gradlew distDockerCopy' from ${REPO_ROOT}"
  fi

  docker build \
    --provenance=false \
    --build-arg "BUILD_DATE=${build_date}" \
    --build-arg "VERSION=${version}" \
    --build-arg "VCS_REF=${git_hash}" \
    -t "${BESU_IMAGE}" \
    "${docker_context}"

  log "Built ${BESU_IMAGE}"
}

ensure_besu_image() {
  if [[ "${SKIP_BUILD_BESU}" == "true" ]]; then
    besu_image_exists || die "Besu image not found: ${BESU_IMAGE} (run ./setup.sh build or omit --no-build-besu)"
    return
  fi

  if [[ "${BUILD_BESU}" == "true" ]]; then
    build_besu_image
    return
  fi

  if is_local_besu_image && ! besu_image_exists; then
    log "Local Besu image not found; building from current branch ..."
    build_besu_image
    return
  fi

  besu_image_exists || die "Besu image not found: ${BESU_IMAGE} (pull it or run ./setup.sh build)"
}

cmd_build() {
  require_cmd docker
  build_besu_image
}

cmd_start() {
  require_cmd docker curl openssl
  resolve_paths
  ensure_besu_image
  download_network_config
  ensure_jwt
  write_env_file
  validate_compose

  local up_args=(up)
  if [[ "${DETACH}" == "true" ]]; then
    up_args+=(-d)
  fi

  log "Starting Besu + Prysm (chain ID ${CHAIN_ID}) ..."
  log "Besu image: ${BESU_IMAGE}"
  log "Besu data: ${BESU_DATA_DIR}"
  log "Prysm image: ${PRYSM_IMAGE}"
  (cd "${SCRIPT_DIR}" && docker compose -f "${SCRIPT_DIR}/docker-compose.yml" "${up_args[@]}")

  if [[ "${DETACH}" == "true" ]]; then
    log ""
    log "Containers started. Watch Besu import blocks with:"
    log "  ${SCRIPT_DIR}/setup.sh logs besu"
    log "Diagnostic nonce/cache/BAL lines only:"
    log "  ${SCRIPT_DIR}/setup.sh logs-diag besu"
    log ""
    log "RPC endpoints:"
    log "  Besu HTTP RPC:  http://127.0.0.1:${BESU_RPC_PORT:-8545}"
    log "  Prysm REST:     http://127.0.0.1:${PRYSM_RPC_PORT:-3500}"
  fi
}

cmd_stop() {
  require_cmd docker
  (cd "${SCRIPT_DIR}" && docker compose -f docker-compose.yml down)
  log "Stopped."
}

ensure_compose_env() {
  if [[ -f "${SCRIPT_DIR}/.env" ]]; then
    return
  fi
  die ".env not found — run ./setup.sh start or ./setup.sh validate first"
}

cmd_restart_prysm() {
  local resync="${1:-false}"
  require_cmd docker
  ensure_compose_env
  preserve_paths_from_env_file
  resolve_paths
  sync_compose_path_env
  verify_besu_jwt_mount

  if [[ "${resync}" == "true" ]]; then
    if [[ "${RESYNC_PRYSM_CONFIRM}" != "true" ]]; then
      die "resync-prysm deletes all Prysm data at ${PRYSM_DATA_DIR}. Pass --confirm to proceed."
    fi
    [[ -n "${PRYSM_DATA_DIR}" ]] || die "PRYSM_DATA_DIR is empty"
    log "Stopping Prysm (Besu keeps running) ..."
    (cd "${SCRIPT_DIR}" && docker compose -f docker-compose.yml stop prysm)
    log "Wiping Prysm data at ${PRYSM_DATA_DIR} ..."
    rm -rf "${PRYSM_DATA_DIR:?}"/*
    log "Starting Prysm fresh (checkpoint sync); Besu is untouched ..."
    (cd "${SCRIPT_DIR}" && docker compose -f docker-compose.yml up -d --no-deps prysm)
  else
    log "Restarting Prysm only (Besu keeps running) ..."
    (cd "${SCRIPT_DIR}" && docker compose -f docker-compose.yml up -d --no-deps --force-recreate prysm)
  fi

  log "Done. Watch Prysm sync with:"
  log "  ${SCRIPT_DIR}/setup.sh logs prysm"
}

cmd_logs() {
  require_cmd docker
  local service="${1:-}"
  (cd "${SCRIPT_DIR}" && docker compose -f docker-compose.yml logs -f ${service:+"$service"})
}

cmd_logs_diag() {
  require_cmd docker
  local service="${1:-besu}"
  log "Following [BAL-DIAG] lines from ${service} (set BESU_LOG_LEVEL=DEBUG for per-tx detail) ..."
  (cd "${SCRIPT_DIR}" && docker compose -f docker-compose.yml logs -f "${service}" 2>&1) \
    | grep -E '\[BAL-DIAG\]'
}

cmd_status() {
  require_cmd docker
  (cd "${SCRIPT_DIR}" && docker compose -f docker-compose.yml ps)
}

parse_args() {
  local command="${1:-start}"
  shift || true

  while [[ $# -gt 0 ]]; do
    case "$1" in
      start|build|stop|restart|restart-prysm|resync-prysm|logs|logs-diag|status|config|validate)
        command="$1"
        shift
        ;;
      -d|--data-dir)
        DATA_DIR="$2"
        shift 2
        ;;
      --besu-data-dir)
        BESU_DATA_DIR="$2"
        shift 2
        ;;
      --prysm-image)
        PRYSM_IMAGE="$2"
        shift 2
        ;;
      --besu-image)
        BESU_IMAGE="$2"
        shift 2
        ;;
      --build-besu)
        BUILD_BESU=true
        shift
        ;;
      --no-build-besu)
        SKIP_BUILD_BESU=true
        shift
        ;;
      --checkpoint-sync-url)
        CHECKPOINT_SYNC_URL="$2"
        shift 2
        ;;
      --p2p-host)
        P2P_HOST="$2"
        shift 2
        ;;
      --bootnodes)
        BESU_BOOTNODES="$2"
        shift 2
        ;;
      --fetch-el-bootnodes)
        FETCH_EL_BOOTNODES=true
        shift
        ;;
      --use-static-nodes)
        BESU_USE_STATIC_NODES=true
        shift
        ;;
      --static-peers)
        EXTRA_STATIC_PEERS="$2"
        shift 2
        ;;
      --max-peers)
        BESU_MAX_PEERS="$2"
        [[ "${BESU_MAX_PEERS}" =~ ^[0-9]+$ ]] || die "--max-peers requires a positive integer, got: ${BESU_MAX_PEERS}"
        shift 2
        ;;
      --refresh-config)
        REFRESH_CONFIG=true
        shift
        ;;
      --foreground)
        DETACH=false
        shift
        ;;
      --prysm-only)
        PRYSM_ONLY=true
        shift
        ;;
      --confirm)
        RESYNC_PRYSM_CONFIRM=true
        shift
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      besu|prysm)
        LOG_SERVICE="$1"
        shift
        ;;
      *)
        die "Unknown argument: $1 (use --help)"
        ;;
    esac
  done

  if [[ "${command}" == "restart-prysm" || "${command}" == "resync-prysm" || "${command}" == "restart" ]]; then
    preserve_paths_from_env_file
  fi
  resolve_paths

  case "${command}" in
    start) cmd_start ;;
    build) cmd_build ;;
    stop) cmd_stop ;;
    restart)
      if [[ "${PRYSM_ONLY}" == "true" ]]; then
        cmd_restart_prysm false
      else
        cmd_stop
        cmd_start
      fi
      ;;
    restart-prysm) cmd_restart_prysm false ;;
    resync-prysm) cmd_restart_prysm true ;;
    logs) cmd_logs "${LOG_SERVICE}" ;;
    logs-diag) cmd_logs_diag "${LOG_SERVICE:-besu}" ;;
    status) cmd_status ;;
    config)
      require_cmd curl jq openssl
      resolve_paths
      download_network_config
      ensure_jwt
      log "Network config ready in ${CONFIG_DIR}"
      ;;
    validate)
      require_cmd docker curl jq openssl
      preserve_paths_from_env_file
      resolve_paths
      download_network_config
      ensure_jwt
      write_env_file
      validate_compose
      ;;
    *) die "Unknown command: ${command}" ;;
  esac
}

# Allow env overrides before CLI parsing
DATA_DIR="${DATA_DIR:-}"
BESU_DATA_DIR="${BESU_DATA_DIR:-}"
PRYSM_IMAGE="${PRYSM_IMAGE:-${DEFAULT_PRYSM_IMAGE}}"
BESU_IMAGE="${BESU_IMAGE:-${DEFAULT_BESU_IMAGE}}"

parse_args "$@"
