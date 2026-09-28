# EIP-8347 migration artifacts (Besu)

Dual-check **verify** of an EIP-8347 artifact pair (PBT snapshot + preimages) against an MPT `stateRoot`, plus offline **convert** that builds a PBT snapshot from an external preimages file and anchor world state.

No BAL, no world-state writes during verify. Convert writes snapshot (and optionally a genesis-derived preimage file); it then dual-checks the result.

## Scope

| Does | Does not |
|------|----------|
| Dual-check verify (PBT root + MPT anchor) | Live migration / storage rewrite |
| Offline snapshot generation from preimages file + anchor state | BAL, reorg, prune |
| CLI `storage pbt verify` / `storage pbt convert` | Stem-grouped / typed snapshot layouts (EIP PR 12379) |

**Snapshot format:** current RLP leaf-record layout
(`pbtRoot[32] | leafCount[8 BE] | RLP([key, value])*`).
Variable-length leaves mean verify still needs a seek index; convert uses an
external merge-sort spill (convert-only — verify streams already-sorted artifacts).

---

## Package layout

Base: `org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347`

```
eip8347/
├── README.md                          (this file)
├── artifact/                          formats + I/O
│   ├── Eip8347SnapshotLeaf
│   ├── Eip8347PreimageRecord
│   ├── Eip8347SnapshotReader
│   ├── Eip8347PreimageReader
│   ├── Eip8347ArtifactWriter
│   └── Eip8347ArtifactVerificationException
├── verify/                            dual-check
│   ├── Eip8347DualCheckVerifier
│   └── Eip8347SnapshotLeafIndex
└── convert/                           snapshot generation
    ├── Eip8347SnapshotGenerator
    ├── Eip8347LeafSpillSorter         (package-private; convert-only)
    ├── Eip8347StateSource
    ├── Eip8347WorldStateSource
    └── Eip8347PreimageFromGenesis     (genesis alloc → preimage file; Hive/tests / CLI fallback)
```

Tests mirror the same subpackages under `ethereum/core/src/test/.../eip8347/{verify,convert}/`.

---

## CLI

### Verify

```bash
besu storage pbt verify \
  --snapshot /path/to/snapshot.bin \
  --preimages /path/to/preimages.bin \
  --anchor 0x…blockHash   # or decimal number
```

Exit: **0** accept, **1** `Eip8347ArtifactVerificationException`, **2** other failure.

### Convert

```bash
# Normal path: external preimages file + anchor world state → snapshot
besu storage pbt convert \
  --preimages /path/to/preimages.bin \
  --snapshot /path/to/snapshot.bin \
  --anchor 0x…blockHash

# Optional: derive preimages from genesis alloc (Bonsai has no keccak preimage store).
# Used for Hive / small test chains; not the mainnet convert input path.
besu storage pbt convert \
  --preimages-out /path/to/preimages.bin \
  --snapshot /path/to/snapshot.bin \
  --anchor 0
```

Convert always dual-checks the written snapshot against the same preimages and anchor `stateRoot` before exit 0.

**Preimages for convert:** the generator reads a preimage **file**. Supply `--preimages`, or omit it and use `--preimages-out` so the CLI writes a file from genesis `alloc` via `Eip8347PreimageFromGenesis` (Hive/tests / local genesis), then feeds that path into the generator.

---

## One `verify` call — how the classes link

### 1. Entry: CLI → orchestrator

1. **`StorageSubCommand`** registers **`PbtSubCommand`** (`storage pbt`).
2. **`PbtSubCommand.Verify`** resolves `--anchor` on the local chain and reads `stateRoot`.
3. Calls **`Eip8347DualCheckVerifier.verify(snapshotPath, preimagesPath, stateRoot)`**.

### 2. Phase A — open resources

| Object | Package | Role |
|--------|---------|------|
| `AscendingCollapseBinaryTrie` | besu-stateless | Live PBT; left-sibling collapse |
| `Eip8347SnapshotReader` | artifact | Streams snapshot leaves; tracks byte offsets |
| `Eip8347SnapshotLeafIndex` | verify | Sparse samples + offsets into the **original** snapshot (**no** value spill file) |
| `Eip8347PreimageReader` | artifact | Streams preimage records (read in Phase C) |

Verify does **not** use `Eip8347LeafSpillSorter`. Artifacts are assumed already sorted; verify streams them.

### 3. Phase B — internal PBT consistency

For each leaf: reader → `Eip8347SnapshotLeaf` → insert into PBT + `SnapshotLeafIndex.record(key, offset)`. Then exhaust/count/root checks; index `seal()`.

### 4. Phase C — consensus anchoring (`anchorToMpt`)

Preimage records drive MPT rebuild via index lookups into the original snapshot (seek+parse). Account + storage `AscendingCollapsePatriciaTrie`; final MPT root ≟ `expectedMptStateRoot`; `ensureAllConsumed()`.

### 5. Data flow (verify)

```
Snapshot file (RLP leaves — single source of truth)
  → Eip8347SnapshotReader → Eip8347SnapshotLeaf
       ├→ AscendingCollapseBinaryTrie.insert  → computed PBT root ≟ claimedRoot
       └→ Eip8347SnapshotLeafIndex.record(key, offset) → seal()

Preimage file
  → Eip8347PreimageReader → Eip8347PreimageRecord (cached hashes)
       → buildAccountRlp using SnapshotLeafIndex lookups (seek + parse one RLP)
            ├ BasicDataEncoder / DelegationEncoder / CodeChunkifier
            └ AscendingCollapsePatriciaTrie (storage, then account)
       → computed MPT root ≟ expectedMptStateRoot
       → SnapshotLeafIndex.ensureAllConsumed()
```

Any format/order/root/coverage failure throws **`Eip8347ArtifactVerificationException`**.  
(`Eip8347ArtifactWriter` is **not** on the verify path — tests/tooling and genesis preimage write only.)

### 6. Sequence diagram (one `verify`)

```mermaid
sequenceDiagram
  autonumber
  actor User
  participant CLI as PbtSubCommand.Verify
  participant V as Eip8347DualCheckVerifier
  participant SR as Eip8347SnapshotReader
  participant Leaf as Eip8347SnapshotLeaf
  participant PBT as AscendingCollapseBinaryTrie
  participant Idx as Eip8347SnapshotLeafIndex
  participant PR as Eip8347PreimageReader
  participant Rec as Eip8347PreimageRecord
  participant Acc as AscendingCollapsePatriciaTrie<br/>(account)
  participant Sto as AscendingCollapsePatriciaTrie<br/>(per-account storage)

  User->>CLI: storage pbt verify
  CLI->>CLI: resolve anchor → stateRoot
  CLI->>V: verify(snapshot, preimages, stateRoot)

  V->>PBT: new
  V->>SR: open snapshot
  V->>Idx: open (path only; no spill file)
  V->>PR: open preimages

  loop each snapshot leaf
    SR->>Leaf: new SnapshotLeaf(key, value)
    V->>PBT: insert(key, value)
    V->>Idx: record(key, lastLeafOffset)
  end
  V->>SR: ensureExhausted()
  V->>PBT: rootHash() ≟ claimedRoot
  V->>Idx: seal()

  V->>Acc: new (account trie)
  loop each preimage record
    PR->>Rec: new PreimageRecord(address, slots)
    Note over V,Idx: buildAccountRlp: require/get basic-data,<br/>code_hash|delegation, CODE_ZONE, storage<br/>(seek into original snapshot)
    opt has storage slots
      V->>Sto: new + insert(slotHash, rlpValue)*
      Sto-->>V: storage rootHash
    end
    V->>Acc: insert(addressHash, accountRlp)
  end
  V->>PR: ensureExhausted()
  V->>Acc: rootHash() ≟ expectedMptStateRoot
  V->>Idx: ensureAllConsumed()
  V-->>CLI: return (accept)
  CLI-->>User: exit 0
```

---

## One `convert` call — how the classes link

### Entry

1. **`PbtSubCommand.Convert`** resolves `--anchor` and loads world state.
2. Resolves preimages path: existing `--preimages`, or writes genesis alloc via **`Eip8347PreimageFromGenesis`** to `--preimages-out`.
3. **`Eip8347SnapshotGenerator.generate(preimages, new Eip8347WorldStateSource(…), snapshot)`**.
4. Dual-check: **`Eip8347DualCheckVerifier.verify(snapshot, preimages, stateRoot)`**.

### Generator + spill (convert-only)

Convert does **not** hold the full leaf set in heap:

1. Stream **`Eip8347PreimageReader`** records.
2. For each account, look up nonce/balance/code/storage via **`Eip8347StateSource`** and emit PBT leaves into **`Eip8347LeafSpillSorter`** (bounded runs on disk).
3. K-way merge runs into the snapshot while hashing with `AscendingCollapseBinaryTrie` → claimed `pbtRoot` + `leafCount` header.

Spill / external merge-sort is **convert-only**. Verify never spills; it streams sorted artifacts and seeks the original snapshot via the leaf index.

### Call graph (convert)

```
besu storage pbt convert
  └─ PbtSubCommand.Convert
       ├─ [optional] Eip8347PreimageFromGenesis.write  → preimage file (genesis alloc)
       ├─ Eip8347SnapshotGenerator.generate
       │    ├─ Eip8347PreimageReader → Eip8347PreimageRecord
       │    ├─ Eip8347WorldStateSource (anchor world state)
       │    └─ Eip8347LeafSpillSorter → snapshot file + claimed pbtRoot
       └─ Eip8347DualCheckVerifier.verify (same as verify CLI)
```

---

## Memory model

| Piece | Path | Footprint |
|-------|------|-----------|
| Snapshot / preimage readers | verify + convert | O(1) outside current record |
| `AscendingCollapseBinaryTrie` | verify + convert merge | O(depth) live |
| `AscendingCollapsePatriciaTrie` | verify only | O(depth) live |
| `Eip8347SnapshotLeafIndex` | verify only | Sparse samples + consumption `BitSet`; **no** value-copy spill |
| `Eip8347LeafSpillSorter` | **convert only** | One run of leaves in heap + one record per open run at merge |
| Code path (verify) | verify | Seek+parse via index; no `seenCodeSizes` map |

Keys must be **strictly ascending** in artifacts: violation → reject (mapped from collapse tries' `IllegalArgumentException`).

---

## Class catalog

### Package `...eip8347.artifact`

| Class | Role |
|-------|------|
| **`Eip8347SnapshotLeaf`** | One leaf: zone key + `Bytes32` value |
| **`Eip8347PreimageRecord`** | `address`, `slotKeys`, cached `addressHash` / `slotKeyHashes` |
| **`Eip8347SnapshotReader`** | Snapshot stream; `lastLeafOffset()`, exhaust checks |
| **`Eip8347PreimageReader`** | Preimage stream; keccak-address / keccak-slot order |
| **`Eip8347ArtifactWriter`** | Snapshot / preimage writing (sort included); tests & genesis write |
| **`Eip8347ArtifactVerificationException`** | Format / dual-check reject → CLI exit 1 |

### Package `...eip8347.verify`

| Class | Role |
|-------|------|
| **`Eip8347DualCheckVerifier`** | Orchestrator: PBT check → `anchorToMpt` |
| **`Eip8347SnapshotLeafIndex`** | Sparse offset index into original snapshot; consumption bitset |

### Package `...eip8347.convert`

| Class | Role |
|-------|------|
| **`Eip8347SnapshotGenerator`** | Preimages file + `StateSource` → PBT snapshot |
| **`Eip8347LeafSpillSorter`** | Bounded-memory external sort of leaves (**convert-only**) |
| **`Eip8347StateSource`** | Read-only account/storage/code view at anchor (test stubs) |
| **`Eip8347WorldStateSource`** | `StateSource` backed by Besu `WorldState` (CLI convert) |
| **`Eip8347PreimageFromGenesis`** | Genesis `alloc` → preimage file (Hive/tests / CLI when `--preimages` omitted) |

### Patricia (module `ethereum/trie`) / besu-stateless

| Class | Role |
|-------|------|
| **`AscendingCollapsePatriciaTrie`** | Streaming MPT (verify anchoring) |
| **`AscendingCollapseBinaryTrie`** | Ascending PBT inserts (verify + convert merge) |
| **`BasicDataEncoder`**, **`DelegationEncoder`**, **`CodeChunkifier`**, **`TrieKeyDerivation`** | Codecs / key derivation |

### CLI

| Class | Role |
|-------|------|
| **`PbtSubCommand`** | Picocli parent (`storage pbt`) |
| **`PbtSubCommand.Verify`** | Dual-check entry |
| **`PbtSubCommand.Convert`** | Snapshot generation + dual-check |
| **`StorageSubCommand`** | Registers `pbt` |

### Call graph (verify)

```
besu storage pbt verify
  └─ PbtSubCommand.Verify
       └─ Eip8347DualCheckVerifier.verify
            ├─ Eip8347SnapshotReader → Eip8347SnapshotLeaf
            ├─ AscendingCollapseBinaryTrie
            ├─ Eip8347SnapshotLeafIndex
            └─ Eip8347PreimageReader → Eip8347PreimageRecord
                 ├─ BasicDataEncoder / DelegationEncoder / CodeChunkifier
                 └─ AscendingCollapsePatriciaTrie
```

---

## Artifact formats (recap)

**Snapshot**: `pbtRoot[32] | leafCount[8 BE] | RLP([key, value])*` — values = canonical RLP integers (pad→32 B on read), keys strictly ascending in PBT order.

**Preimages**: concat records `address[20] | slotCount[4 BE] | slotKey[32]*` — sorted by `keccak256(address)`; slots by `keccak256(slotKey)`.

## Tests

| Test | Package |
|------|---------|
| `Eip8347DualCheckVerifierTest` | `...eip8347.verify` — fixtures via `Eip8347ArtifactWriter`, accept/reject |
| `Eip8347SnapshotGeneratorTest` | `...eip8347.convert` — generate + dual-check; spill capacity |
| `Eip8347PreimageFromGenesisTest` | `...eip8347.convert` — genesis alloc ordering / zero-slot skip |
