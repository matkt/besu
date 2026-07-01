# Glamsterdam Devnet 6 — Besu + Prysm (Docker)

Run a local **Besu** execution client and **Prysm** beacon node connected to [Glamsterdam devnet 6](https://glamsterdam-devnet-6.ethpandaops.io/), an Ethereum protocol devnet for the Glamsterdam upgrade (ePBS / EIP-7732, BAL / EIP-7928).

Besu flags align with the Glamsterdam devnet reference configuration: `FULL` sync, `BONSAI` storage, metrics on port `6060`, and the default EL bootnode list.

## Prerequisites

- Docker Engine with Compose v2 (`docker compose`)
- `curl`, `openssl`, `jq`
- Outbound internet access (to download genesis/config and peer with the devnet)
- **Besu from source:** JDK and Gradle (via `./gradlew`) when building the local Besu image — first `./setup.sh start` auto-builds if the image is missing

## Quick start

```bash
cd docker/glamsterdam-devnet6
chmod +x setup.sh

# Start with Besu built from the current repo branch (auto-builds on first run)
./setup.sh start

# Custom data directory — still uses branch Besu by default
./setup.sh start --data-dir /tmp/besu

# Watch Besu import blocks (EL sync progress)
./setup.sh logs besu
```

### Public / routable node

For a host with a public IP (advertised in enode), set `P2P_HOST` to that address:

```bash
P2P_HOST=206.189.200.7 ./setup.sh start --data-dir /path/to/data
# or
./setup.sh start --p2p-host 206.189.200.7 --data-dir /path/to/data
```

Default `P2P_HOST` is `0.0.0.0` (suitable for local Docker).

### Building Besu from the current branch

By default, Besu runs from a **local Docker image** built from this repository (`besu:glamsterdam-devnet6-local`), not the ethPandaOps image. Prysm and network genesis/config still come from ethPandaOps.

```bash
# Build only (from repo root context via gradlew distDockerCopy + docker build)
./setup.sh build

# Force rebuild before start (after pulling new commits or changing branches)
./setup.sh start --build-besu --data-dir /tmp/besu

# Use ethPandaOps pre-built Besu instead
./setup.sh start --besu-image ethpandaops/besu:glamsterdam-devnet-6
```

Manual build (equivalent to `./setup.sh build`):

```bash
cd ../..   # repo root
./gradlew distDockerCopy
docker build \
  --provenance=false \
  --build-arg BUILD_DATE="$(date -u +"%Y-%m-%dT%H:%M:%SZ")" \
  --build-arg VERSION="$(./gradlew -q properties | awk -F': ' '/^version:/ {print $2}')" \
  --build-arg VCS_REF="$(git rev-parse --short=7 HEAD)" \
  -t besu:glamsterdam-devnet6-local \
  build/docker-besu
```

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--data-dir` | `~/.glamsterdam-devnet6` | Base host path; Besu data at `<data-dir>/besu` |
| `--besu-data-dir` | `<data-dir>/besu` | Override Besu data mount only |
| `--prysm-image` | `ethpandaops/prysm-beacon-chain:glamsterdam-devnet-6` | Prysm Docker image |
| `--besu-image` | `besu:glamsterdam-devnet6-local` | Besu Docker image (local branch build) |
| `--build-besu` | off | Build/rebuild local Besu image before `start` |
| `--no-build-besu` | off | Do not auto-build; fail if local image is missing |
| `--p2p-host` | `0.0.0.0` | Besu `--p2p-host` (public IP for routable nodes) |
| `--bootnodes` | Glamsterdam devnet 6 list | Override EL `--bootnodes` |
| `--fetch-el-bootnodes` | off | Fetch EL bootnodes from inventory API instead |
| `--use-static-nodes` | off | Enable `--static-nodes-file=/config/static-nodes.json` |
| `--checkpoint-sync-url` | bootnode beacon API | Prysm checkpoint sync endpoint |
| `--static-peers` | (none) | Extra EL enodes appended to `static-nodes.json` |
| `--max-peers` | (none) | Optional Besu `--max-peers` |
| `--refresh-config` | off | Re-download genesis/config from ethPandaOps |

### Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `P2P_HOST` | `0.0.0.0` | Besu advertised P2P host |
| `BESU_BOOTNODES` | Glamsterdam devnet 6 list | EL bootnodes (comma-separated) |
| `BESU_USE_STATIC_NODES` | `false` | Set `true` to enable static EL peers file |
| `FETCH_EL_BOOTNODES` | `false` | Set `true` to fetch EL bootnodes from inventory |
| `BESU_MAX_PEERS` | (unset) | Optional Besu peer limit |
| `BESU_METRICS_PORT` | `6060` | Host port mapped to Besu metrics |

### Examples

```bash
# Custom Besu data directory on the host (local branch Besu)
./setup.sh start --data-dir /var/lib/glamsterdam6

# Public node with explicit P2P host
P2P_HOST=206.189.200.7 ./setup.sh start --data-dir /var/lib/glamsterdam6

# Rebuild Besu after switching branches, then start
./setup.sh start --build-besu --data-dir /tmp/besu

# Pin ethPandaOps Besu + Prysm images instead of local build
./setup.sh start \
  --data-dir /tmp/glamsterdam6 \
  --prysm-image ethpandaops/prysm-beacon-chain:glamsterdam-devnet-6 \
  --besu-image ethpandaops/besu:glamsterdam-devnet-6

# Refresh network files after a devnet reset
./setup.sh start --refresh-config

# Enable static EL peers (inventory enodes + optional --static-peers)
./setup.sh start --use-static-nodes --refresh-config

# Fetch EL bootnodes from inventory instead of the baked-in default list
./setup.sh start --fetch-el-bootnodes

# Validate compose without starting
./setup.sh validate

# Restart after config changes
./setup.sh restart

# Restart Prysm only (Besu keeps running and syncing)
./setup.sh restart-prysm
# equivalent:
./setup.sh restart --prysm-only

# Wipe Prysm data and restart checkpoint sync (Besu untouched; requires --confirm)
./setup.sh resync-prysm --confirm

# Stop containers (data preserved)
./setup.sh stop
```

## Besu flags (reference-aligned)

| Flag | Value in Docker |
|------|-----------------|
| `--data-path` | `/var/lib/besu` (host: `<data-dir>/besu`) |
| `--genesis-file` | `/config/besu.json` |
| `--engine-jwt-secret` | `/execution-auth.jwt` (shared with Prysm) |
| `--engine-rpc-enabled` | `true` (Engine API on port 8551) |
| `--sync-mode` | `FULL` |
| `--data-storage-format` | `BONSAI` |
| `--bonsai-limit-trie-logs-enabled` | `false` |
| `--nat-method` | `NONE` |
| `--metrics-port` | `6060` |
| `--rpc-http-api` | `ADMIN,DEBUG,ETH,MINER,NET,TRACE,TXPOOL,WEB3` |

## Network parameters

| Field | Value | Source |
|-------|-------|--------|
| Chain ID | `7052886157` | [EL genesis](https://config.glamsterdam-devnet-6.ethpandaops.io/el/besu.json) |
| Deposit contract | `0x00000000219ab540356cBB839Cbe05303d7705Fa` | [CL config](https://config.glamsterdam-devnet-6.ethpandaops.io/cl/config.yaml) |
| Deposit contract block | `0` | ethPandaOps config API |
| EL genesis | `besu.json` | `config.glamsterdam-devnet-6.ethpandaops.io/el/besu.json` |
| CL genesis | `genesis.ssz` + `config.yaml` | ethPandaOps config API |
| EL bootnodes | Default Glamsterdam list (6 enodes) | Baked into `setup.sh`; override with `BESU_BOOTNODES` |
| CL bootnodes | Fetched at startup (5 CL ENRs) → `prysm-bootnodes.yaml` | [Node inventory API](https://config.glamsterdam-devnet-6.ethpandaops.io/api/v1/nodes/inventory); Prysm needs one ENR per YAML list entry (comma-separated CLI values are invalid) |
| Static EL peers | Optional (`--use-static-nodes`) | Inventory API → `static-nodes.json` |
| Checkpoint sync | `https://checkpoint-sync.glamsterdam-devnet-6.ethpandaops.io` | ethPandaOps bootnode |

Fork highlights (from genesis): merge at block 0, **Amsterdam** fork at timestamp `1782398520` (Glamsterdam EL features).

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  Docker network: glamsterdam-devnet6                      │
│                                                           │
│  ┌──────────────┐  Engine API (8551)   ┌──────────────┐  │
│  │    Besu      │◄──── JWT auth ──────►│    Prysm     │  │
│  │  (EL/sync)   │                      │  (CL/sync)   │  │
│  └──────┬───────┘                      └──────┬───────┘  │
│         │ p2p 30303                            │ p2p     │
└─────────┼──────────────────────────────────────┼──────────┘
          │                                      │
          ▼                                      ▼
   Glamsterdam devnet 6 peers (ethPandaOps)
```

### Exposed ports

| Service | Port | Purpose |
|---------|------|---------|
| Besu | 8545 | HTTP JSON-RPC |
| Besu | 8551 | Engine API (JWT) |
| Besu | 30303 | EL P2P (TCP/UDP) |
| Besu | 6060 | Metrics |
| Prysm | 3500 | REST / gRPC gateway |
| Prysm | 4000 | gRPC |
| Prysm | 13000/12000 | CL P2P |

## Seeing block imports

Besu logs block import progress at `INFO` level during sync:

```bash
./setup.sh logs besu
```

Look for lines mentioning imported blocks / sync progress. Prysm waits until Besu passes its health check (RPC responding) before starting.

## Restart Prysm without touching Besu

When Prysm CL sync stalls or needs a restart, you can restart or resync Prysm alone. Besu keeps running and its data directory is never touched.

| Command | Besu | Prysm data | Use when |
|---------|------|------------|----------|
| `restart-prysm` | keeps running | preserved | Prysm hung, config tweak, quick restart |
| `restart --prysm-only` | keeps running | preserved | same as `restart-prysm` |
| `resync-prysm --confirm` | keeps running | **wiped** | fresh Prysm checkpoint sync after CL issues |

```bash
# Restart Prysm container only
./setup.sh restart-prysm

# Same via restart flag
./setup.sh restart --prysm-only

# Wipe Prysm datadir and restart checkpoint sync (Besu untouched)
./setup.sh resync-prysm --confirm

# Custom data directory
./setup.sh restart-prysm --data-dir /var/lib/glamsterdam6
./setup.sh resync-prysm --confirm --data-dir /var/lib/glamsterdam6
```

`resync-prysm` requires `--confirm` to avoid accidental Prysm data loss. It stops Prysm, clears `<data-dir>/prysm`, then runs `docker compose up -d --no-deps prysm`. `restart-prysm` also recreates the Prysm container (not just `restart`) so volume mounts pick up the correct JWT path from `--data-dir` / `.env`.

## Static peers (optional)

By default Besu uses **bootnodes only** (matching the reference config). For persistent outbound connections to all inventory EL nodes, enable static peers:

```bash
./setup.sh start --use-static-nodes --refresh-config
```

Generated file: `<data-dir>/config/static-nodes.json`

```bash
# Inspect generated static peers
cat ~/.glamsterdam-devnet6/config/static-nodes.json | jq 'length'

# Extra enodes via env or CLI (merged with inventory)
EXTRA_STATIC_PEERS="enode://..." ./setup.sh start --use-static-nodes
./setup.sh start --use-static-nodes --static-peers "enode://...@host:30303"
```

## Caveats

1. **Local Besu vs devnet images** — By default, Besu is built from the **current repo branch** (`besu:glamsterdam-devnet6-local`). Prysm still uses `ethpandaops/prysm-beacon-chain:glamsterdam-devnet-6`. Network genesis/config always come from ethPandaOps. Use `--besu-image ethpandaops/besu:glamsterdam-devnet-6` if you prefer the published devnet image.

2. **Devnet can reset** — ethPandaOps may redeploy devnet 6. If sync stalls after an upgrade, run `./setup.sh stop`, clear Besu/Prysm data dirs if needed, then `./setup.sh start --refresh-config`. To retry Prysm checkpoint sync without restarting Besu, use `./setup.sh resync-prysm --confirm`.

3. **P2P reachability** — Port `30303` (EL) and `13000/tcp` + `12000/udp` (CL) must be reachable for good peering. Set `P2P_HOST` to your public IP when running a routable node. Prysm bootnodes are written to `<data-dir>/config/prysm-bootnodes.yaml` (not comma-separated in one flag).

4. **Prysm has no CL peers (`total=0`)** — If logs show `Waiting for enough suitable peers` with `total=0`, regenerate bootnodes and recreate Prysm: `./setup.sh validate --data-dir <dir>` then `./setup.sh restart-prysm --data-dir <dir>`. Default `--min-sync-peers` is `1` for this devnet setup (override with `PRYSM_MIN_SYNC_PEERS`).

5. **No validator** — This setup runs a full node pair only. Running a validator requires separate key material from ethPandaOps.

6. **JWT secret** — Shared `execution-auth.jwt` is generated once under `<data-dir>/jwt/` (64 hex chars, no trailing newline). Both Besu and Prysm mount the same host file at `/execution-auth.jwt`. **Use the same `--data-dir` for every command** (`start`, `restart-prysm`, `resync-prysm`); if `.env` is regenerated with a different data directory while Besu is still running, Prysm can mount a different JWT and Engine API auth fails with `401 Unauthorized`. After regenerating the JWT, restart **both** Besu and Prysm. `restart-prysm` / `resync-prysm` refuse to run when the running Besu container's JWT mount does not match `.env`.

### JWT auth failure (`401 Unauthorized`)

If Prysm logs `HTTP authentication to your execution client is not working` / `401 Unauthorized`:

```bash
# 1. Check which JWT file the running Besu container actually uses
docker inspect glamsterdam6-besu --format '{{range .Mounts}}{{if eq .Destination "/execution-auth.jwt"}}{{.Source}}{{end}}{{end}}'

# 2. Stop and restart BOTH with the same data directory Besu was started with
./setup.sh stop
./setup.sh start --data-dir /tmp/besu   # example: match your Besu data dir

# Or restart Prysm only once paths match (setup.sh verifies the mount)
./setup.sh restart-prysm --data-dir /tmp/besu
```

## References

- [Glamsterdam devnet 6 dashboard](https://glamsterdam-devnet-6.ethpandaops.io/)
- [Network spec (HackMD)](https://notes.ethereum.org/@ethpandaops/glamsterdam-devnet-6)
- [ethPandaOps glamsterdam-devnets](https://github.com/ethpandaops/glamsterdam-devnets)
- [Besu CLI — Engine API](https://besu.hyperledger.org/reference/cli/options#engine-api)
- [Prysm beacon-chain flags](https://docs.prylabs.network/docs/prysm-usage/parameters)
