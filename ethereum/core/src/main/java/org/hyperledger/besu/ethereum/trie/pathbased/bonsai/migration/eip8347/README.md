# EIP-8347 dual-check verify (Besu)

**Verify-only** check of an EIP-8347 artifact pair (PBT snapshot + preimages) against the MPT `stateRoot` of an anchor block.  
No Bonsai→PBT conversion, no BAL, no world-state writes — accept/reject only.

## Scope

| Does | Does not |
|------|----------|
| Internal PBT consistency (leaf stream → root) | Mainnet artifact generation / export |
| Consensus anchoring (preimages + leaves → MPT `stateRoot`) | Live migration / storage conversion |
| CLI exit codes 0 / 1 / 2 | BAL, reorg, prune |

Input: `snapshotPath`, `preimagesPath`, `expectedMptStateRoot` (`Bytes32`).  
Output: void, or `Eip8347ArtifactVerificationException`.

**Snapshot format:** stay on the current RLP leaf-record layout
(`pbtRoot[32] | leafCount[8 BE] | RLP([key, value])*`).
EIP PR 12379 (stem-grouped / typed layouts) is **out of scope** here and does **not**
remove the need for a seek index — leaves remain variable-length with no embedded
offset table.

---

## One `verify` call — how the classes link

This section walks a single dual-check from CLI entry to accept/reject. Every class named below exists in the current tree.

### 1. Entry: CLI → orchestrator

```
besu storage pbt verify --snapshot … --preimages … --anchor …
```

1. **`StorageSubCommand`** registers **`PbtSubCommand`** (`storage pbt`).
2. **`PbtSubCommand.Verify`** builds a `BesuController`, resolves `--anchor` (block hash `0x…` or decimal number) on the local chain, and reads that header’s `stateRoot`.
3. It calls **`Eip8347DualCheckVerifier.verify(snapshotPath, preimagesPath, stateRoot)`**.
4. Exit mapping:
   - **0** — `verify` returns normally (accept).
   - **1** — `Eip8347ArtifactVerificationException` (artifact reject).
   - **2** — any other failure (I/O, unknown anchor, controller errors, …).

From here on, everything is inside `verify`.

### 2. Phase A — open resources

`verify` constructs, in a try-with-resources:

| Object | Who opens it | Role for this call |
|--------|--------------|--------------------|
| `AscendingCollapseBinaryTrie` (lib `partitionedbinarytrie`) | verifier | Live PBT; left-sibling collapse → O(depth) heap |
| `Eip8347SnapshotReader` | verifier | Streams snapshot leaves one at a time; tracks byte offsets |
| `Eip8347SnapshotLeafIndex` | verifier | Sparse key samples + offsets into the **original** snapshot (no value spill file) |
| `Eip8347PreimageReader` | verifier | Streams preimage records (opened now; read in Phase C) |

The binary trie is **not** a Besu wrapper — the verifier uses `AscendingCollapseBinaryTrie` from `besu-stateless` directly.

### 3. Phase B — internal PBT consistency

For each leaf from the snapshot:

1. **`Eip8347SnapshotReader`** reads the next RLP `[key, value]`, left-pads `value` to 32 bytes, enforces strictly ascending PBT keys and canonical RLP, and returns an **`Eip8347SnapshotLeaf`** (rejects zero values — EIP-8297 absence).
2. The verifier inserts `(key, value)` into **`AscendingCollapseBinaryTrie`** via `insertPbt` (maps the trie’s `IllegalArgumentException` on key-order violations to `Eip8347ArtifactVerificationException`).
3. In parallel, **`Eip8347SnapshotLeafIndex.record(key, offset)`** stores the leaf’s byte offset in the snapshot (`snapshot.lastLeafOffset()`), with a sparse key sample every 1024 leaves.

After the stream:

- `snapshot.ensureExhausted()` — header `leafCount` matches bytes read; no trailing junk.
- `pbt.insertCount()` must equal `snapshot.leafCount()`.
- `pbt.rootHash()` must equal `snapshot.claimedRoot()` (header `pbtRoot`).

On success the leaf index is **`seal()`**ed (consumption bitset allocated; snapshot reopened read-only for seeks). The binary trie is no longer needed for Phase C; the sealed index is the lookup surface for anchoring.

### 4. Phase C — consensus anchoring (`anchorToMpt`)

The verifier creates one **`AscendingCollapsePatriciaTrie`** for the **account** trie, then iterates preimages:

1. **`Eip8347PreimageReader`** yields an **`Eip8347PreimageRecord`** per account (`address`, `slotKeys`). The record constructor caches **`addressHash`** and **`slotKeyHashes`** once so the merge path does not rehash. The reader enforces ascending `keccak(address)` across records and ascending `keccak(slotKey)` within a record.
2. For each record, **`buildAccountRlp(record, leaves)`** materializes a classic RLP account value via index lookups into the snapshot:
   - **Basic data** — `leaves.require(TrieKeyDerivation.getTreeKeyForBasicData(…))` → **`BasicDataEncoder.decodeBasicData`** → nonce, balance, `code_size`.
   - **Code hash vs delegation** — optional `get` of code-hash and delegation tree keys:
     - Delegation present → must not also have code-hash; `code_size` must be 23; leaf checked with **`DelegationEncoder`** / EIP-7702 designator; account `codeHash` = `keccak(designator ‖ target)`.
     - Code-hash present → if `code_size > 0`, **`verifyOneCode`** reassembles bytecode from **CODE_ZONE** chunks via the index (using the account’s claimed `code_size` only — **no** heap `code_hash → code_size` / `seenCodeSizes` map). Chunks are re-checked with **`CodeChunkifier.chunkifyCode`**. Shared bytecode may be looked up again; cost is seek+parse, not a retained map.
     - Neither leaf → reject.
   - **Storage root** — **`buildStorageRoot`**: if no slots, `EMPTY_TRIE_HASH`; else a fresh **`AscendingCollapsePatriciaTrie`**, one insert per slot (`slotKeyHashes[i]` → RLP storage value from `leaves.get(storage tree key)`).
3. The account RLP is inserted into the account Patricia trie under `record.addressHash()` (`insertMpt` again maps order failures to verification exceptions). Internally the Patricia trie uses **`AscendingCollapsePutVisitor`**.

After all preimages:

- `preimages.ensureExhausted()`.
- `accountTrie.rootHash()` must equal the CLI-supplied **`expectedMptStateRoot`**.
- **`leaves.ensureAllConsumed()`** — every indexed snapshot leaf must have been touched by a lookup during anchoring (no orphan PBT leaves).

### 5. Data flow (summary)

```
Snapshot file (RLP leaves — single source of truth)
  → Eip8347SnapshotReader → Eip8347SnapshotLeaf
       ├→ AscendingCollapseBinaryTrie.insert  → computed PBT root ≟ claimedRoot
       └→ Eip8347SnapshotLeafIndex.record(key, offset) → seal()

Preimage file
  → Eip8347PreimageReader → Eip8347PreimageRecord (cached hashes)
       → buildAccountRlp using SnapshotLeafIndex lookups (seek + parse one RLP)
            ├ BasicDataEncoder.decodeBasicData
            ├ DelegationEncoder / CodeChunkifier (as needed)
            └ AscendingCollapsePatriciaTrie (storage, then account)
       → computed MPT root ≟ expectedMptStateRoot
       → SnapshotLeafIndex.ensureAllConsumed()
```

Any format/order/root/coverage failure throws **`Eip8347ArtifactVerificationException`**.  
(`Eip8347ArtifactWriter` is **not** on this path — tests/tooling only.)

There is **no** second leaf spill that rewrites keys/values. Phase-2 `get(key)` opens the
original snapshot path, binary-searches sparse samples, seeks to the sample offset, and
scans RLP records until the key matches.

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

## Memory model

| Piece | Role | Footprint |
|-------|------|-----------|
| Snapshot / preimage readers | Stream, 1 item at a time | O(1) outside current record |
| `AscendingCollapseBinaryTrie` | PBT inserts, collapse left siblings | O(depth) live |
| `AscendingCollapsePatriciaTrie` | Account / storage MPT inserts | O(depth) live |
| `Eip8347SnapshotLeafIndex` | Sparse samples (key+offset+index every 1024) + consumption `BitSet` | Heap ≈ samples + bitset; **no** value-copy spill; values stay in the original snapshot |
| Code path | Seek+parse via index; re-check if bytecode is shared | No `seenCodeSizes` |

**Heap residual after this change:** phase-2 no longer holds a rewritten leaf file or per-leaf values in heap. Residual index heap is ~`(N/1024) × (key + 16 B)` plus `BitSet` of `N` bits (~`N/8` bytes). EIP PR 12379 does not change that need while RLP leaves stay variable-length.

Keys must be **strictly ascending**: violation → reject (mapped from collapse tries' `IllegalArgumentException`).

---

## Class catalog

### Package `...migration.eip8347`

| Class | Role | During verification |
|-------|------|---------------------|
| **`Eip8347DualCheckVerifier`** | Orchestrator: `verify` → PBT check → `anchorToMpt` | **Called by** `PbtSubCommand.Verify`. **Calls** readers, `AscendingCollapseBinaryTrie`, leaf index, `AscendingCollapsePatriciaTrie`; uses lib codecs (`BasicDataEncoder`, `DelegationEncoder`, `CodeChunkifier`, `TrieKeyDerivation`). |
| **`Eip8347SnapshotReader`** | Snapshot stream: `pbtRoot[32] \| leafCount[8 BE] \| RLP([key,value])*`; exposes `lastLeafOffset()` | **Opened by** verifier. **Produces** `Eip8347SnapshotLeaf`. |
| **`Eip8347SnapshotLeaf`** | One leaf: zone key + `Bytes32` value | Constructed by snapshot reader; consumed by verifier inserts. |
| **`Eip8347PreimageReader`** | Preimage stream; keccak-address / keccak-slot order | **Opened by** verifier; iterated in `anchorToMpt`. **Produces** `Eip8347PreimageRecord`. |
| **`Eip8347PreimageRecord`** | `address`, `slotKeys`, cached `addressHash` / `slotKeyHashes` | Built by preimage reader; used for MPT keys and storage lookups. |
| **`Eip8347SnapshotLeafIndex`** | Sparse offset index into original snapshot; `seal`, `get`/`require`, consumption bitset | Filled in Phase B; keyed seek+parse + `ensureAllConsumed` in Phase C. |
| **`Eip8347ArtifactWriter`** | Snapshot / preimage writing (sort included) | **Not called** during verify — tests & tooling only. |
| **`Eip8347ArtifactVerificationException`** | Dual-check / format reject | Thrown by verifier, readers, leaf, record, leaf index; mapped to CLI exit 1. |

### Patricia (module `ethereum/trie`)

| Class | Role | During verification |
|-------|------|---------------------|
| **`AscendingCollapsePatriciaTrie`** | Streaming MPT: `insert` / `insertCount` / `rootHash` | One instance for accounts; one per non-empty storage trie. **Called by** verifier only. |
| **`AscendingCollapsePutVisitor`** | Collapses left siblings to `StoredNode` stubs | Used **internally** by the Patricia wrapper (not referenced by the eip8347 package). |

### External lib `besu-stateless` (`partitionedbinarytrie`)

| Class | Role | During verification |
|-------|------|---------------------|
| **`AscendingCollapseBinaryTrie`** (+ its internal collapse visitor) | Ascending PBT inserts | **Constructed and driven directly** by the verifier (no Besu wrapper). |
| **`BasicDataEncoder`**, **`DelegationEncoder`**, **`CodeChunkifier`**, **`TrieKeyDerivation`**, **`EmbeddingParameters`** | Codecs / key derivation | Used inside `buildAccountRlp` / `verifyOneCode` / leaf key validation. |

### CLI

| Class | Role | During verification |
|-------|------|---------------------|
| **`PbtSubCommand`** | Picocli parent (`storage pbt`) | Dispatches to `Verify`. |
| **`PbtSubCommand.Verify`** | Resolves anchor, calls `verify`, exit codes | **Entry point** for a dual-check run. |
| **`StorageSubCommand`** | Registers `pbt` | Wiring only. |

### Call graph (who calls whom)

```
besu storage pbt verify
  └─ PbtSubCommand.Verify
       └─ Eip8347DualCheckVerifier.verify
            ├─ Eip8347SnapshotReader → Eip8347SnapshotLeaf
            ├─ AscendingCollapseBinaryTrie          (besu-stateless)
            ├─ Eip8347SnapshotLeafIndex             (seek into snapshot)
            └─ Eip8347PreimageReader → Eip8347PreimageRecord
                 ├─ BasicDataEncoder.decodeBasicData / DelegationEncoder / CodeChunkifier
                 └─ AscendingCollapsePatriciaTrie   (ethereum/trie)
                      └─ AscendingCollapsePutVisitor
```

---

## CLI

```bash
besu storage pbt verify \
  --snapshot /path/to/snapshot.bin \
  --preimages /path/to/preimages.bin \
  --anchor 0x…blockHash   # or decimal number
```

Required options: `--snapshot`, `--preimages`, `--anchor`.  
The node must be able to open the data dir and resolve the anchor header.

## Artifact formats (recap)

**Snapshot**: `pbtRoot[32] | leafCount[8 BE] | RLP([key, value])*` — values = canonical RLP integers (pad→32 B on read), keys strictly ascending in PBT order.

**Preimages**: concat records `address[20] | slotCount[4 BE] | slotKey[32]*` — sorted by `keccak256(address)`; slots by `keccak256(slotKey)`.

## Tests

`ethereum/core/src/test/.../eip8347/Eip8347DualCheckVerifierTest.java` — fixtures via `Eip8347ArtifactWriter`, accept/reject cases (order, roots, code, delegation, unconsumed leaves).
