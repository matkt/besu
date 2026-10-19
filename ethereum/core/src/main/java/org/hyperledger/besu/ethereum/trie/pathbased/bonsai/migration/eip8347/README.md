# EIP-8347 migration artifacts

Besu's implementation of the [EIP-8347](https://eips.ethereum.org/EIPS/eip-8347) artifacts, with
the typed, stem-grouped snapshot format of EIP PR 12379, `pbtRoot` at the end (EIP PR 12403), and
tagged records instead of section counts (EIP PR 12404).

## What it is for

At the PBT fork, every node needs the pre-fork state as a PBT (the binary trie of EIP-8297)
instead of an MPT. A node can build it alone by replaying the chain from genesis into its
binary-trie column, which is slow. EIP-8347 lets it start instead from a **PBT snapshot** taken at
a finalized pre-fork **anchor** block.

The node trusts nothing about that snapshot, only the anchor's `stateRoot`, which consensus
validated. The **dual check** proves that the snapshot is exactly the state that `stateRoot`
commits to. The two tries index the state differently:

| | MPT (what the header commits) | PBT (what the snapshot holds) |
|---|---|---|
| Account key | `keccak(address)` | `ACCOUNT_ZONE ‖ blake3(address32) ‖ subIndex` |
| Slot key | `keccak(slotKey)`, in the account's storage trie | a header leaf `ACCOUNT_ZONE ‖ blake3(address32) ‖ (64 + slot)` if `slot < 64`, else `STORAGE_ZONE ‖ blake3(address32) ‖ blake3(address32 ‖ slot / 256) ‖ slot % 256` |
| Order | keccak order | BLAKE3 order |

Hashes cannot be inverted, so going from a PBT key to an MPT key needs the **preimages**: the
addresses and slot keys. This package provides **convert** (anchor state and preimages → snapshot)
and **verify** (snapshot and preimages → accept or reject against the anchor's `stateRoot`), both
run offline from the CLI. In the node, the PBT migrator bootstraps with `verifyAndLoad`, which
verifies the snapshot and writes the PBT into sorted files in the same pass. The files are added to
the binary-trie column once the snapshot is accepted.

## The two files

**Preimages**: `address[20] | slotCount[4] | slotKey[32] × slotCount` per account, accounts sorted
by `keccak256(address)` and slots by `keccak256(slotKey)`. This is the exact list of accounts and
non-zero slots of the anchor state, in MPT order.

**Snapshot**: `record × | end | pbtRoot[32]`, every record opening with a one-byte tag.

| Tag | Record |
|---|---|
| `0x00`, `0x01`, `0x02` | **header record**: one account's whole header stem. The tag is the account `kind`: no code, code (`codeHash`, `codeSize`), or EIP-7702 delegation (20-byte `target`). Then `addressHash`, nonce, balance, and storage slots 0–63 |
| `0x03` | **code group**: one stem of code chunks, `stemHash` then `(subIndex, value)` pairs |
| `0x04` | **storage account**: the `addressHash` the storage groups after it belong to |
| `0x05` | **storage group with one leaf** |
| `0x06` | **storage group with more than one leaf** |
| `0x07` | **end**, followed by `pbtRoot` |

Records come in PBT key order: headers, then code groups, then storage. With tags and the root at
the end, a producer writes the file in the same pass that hashes the PBT, with no section count to
patch and no storage record to buffer.

The records hold typed fields rather than raw leaves, and the PBT leaves are derived from them. One
state gives exactly one file, so the reader rejects anything non-canonical: records or entries not
in strictly ascending order, non-minimal integers, zero values, empty accounts (EIP-7523), an
unknown tag, a storage account with no group after it or a group with none before it, a one-leaf
group in the multi-leaf encoding, a missing end tag, or trailing bytes after the root. The layouts
are in [File layouts](#file-layouts).

## Importing a snapshot into a node

1. **Configuration.** `--Xpbt-snapshot-file`, `--Xpbt-preimages-file` and
   `--Xpbt-snapshot-anchor-block-hash` become a `PbtMigrator.SnapshotBootstrap`, with the work
   directory `<data-path>/pbt-migration`.
2. **Waiting for the anchor.** The migrator ticks every second. While the binary column has no
   **cursor** (the block whose state it holds), it waits until the anchor is known, canonical,
   finalized and before the fork.
3. **Cleaning up.** It empties the binary column and the work directory: with no cursor, whatever
   they hold comes from an interrupted attempt.
4. **Verify and load.** `Eip8347DualCheckVerifier.verifyAndLoad` writes the PBT through a
   `SnapshotColumnWriter` into SST files, with the binary column's own options, under
   `<data-path>/pbt-migration/pbt-column`. Nothing touches the database yet.
5. **Accepted.** `writer.finish(root, anchorHash)` adds the files to the binary column, then writes
   the **cursor, last**.
6. **Rejected** (`Eip8347ArtifactVerificationException`). The files are deleted and the migrator
   stops: reading the same file again would give the same rejection. The column was not touched.
7. **Any other failure** (disk full, I/O…). The files are deleted, the column is emptied, and the
   load is retried 5 minutes later.
8. **Node shutdown.** The load is interrupted, at the latest between two ingestions, and the node
   waits for it, at most a minute, before it closes the database. Stopped before its cursor, the
   load empties the column.

The migrator holds the binary-column lock (`PbtColumnOwnership`) for the whole load. If the chain
reaches the fork meanwhile, it waits for the load to end.

**Crash safety** rests on one rule: no cursor means the column is not valid. A crash mid-load leaves
no cursor, even after the files were added, so on restart the column and the work directory are
emptied and the load starts over.

**After the import**, the migrator follows the chain from the anchor, at most 128 blocks per roll:
through trie logs when the node has them, otherwise through each block's BAL (EIP-7928), stored
locally or fetched from peers. At the fork, the chain claims the column and the migrator retires.

## How verify works

`run` creates a temporary sub-directory of the work directory and four external sorts:

| Sort | Key → value |
|---|---|
| `preimage-requests` | PBT key → rank in the preimage file |
| `mpt-values` | rank → value (a slot value, or an account's fields) |
| `code-group-requests` | code group stem → `codeHash, group, codeSize` |
| `code-groups-by-code` | `codeHash ‖ group` → `codeSize, references, chunks` |

1. **Requests.** The preimages are read without computing any keccak. Each entry gets its **rank**,
   its position in the file (an account's slots first, then the account), and becomes a request
   `PBT key → rank`. Sorted, the requests are in PBT order, the order of the snapshot.
2. **One pass over the snapshot**, as a pipeline:

   ```text
   reader ──► hash stems (N threads, order kept) ──► attach to the PBT ──► join
   ```

   Each unit is one whole stem. `AscendingCollapseBinaryTrie.prepare` hashes it on a worker, and
   `insert` attaches its top node in order; when loading, its nodes go to the column writer. The
   join walks the sorted requests alongside: each header, header slot and storage leaf must match
   its request, and emits `rank → value` into `mpt-values`. A `kind 0x01` header also requests the
   code groups its `codeSize` spans. Each code group must match a request; identical requests
   (accounts sharing a code) are folded, and their count is the code's reference count.
3. **End of the pass.** The reader must be at the end of the file, the computed PBT root must equal
   `pbtRoot` (**check 1**; when loading, the root node is written now), and no request may be left.
4. **Two checks at the same time:**
   - **Check 2, consensus anchoring.** `mpt-values` sorted by rank is back in preimage order, that
     is MPT order. The preimages are read again, checking the keccak order. A worker builds each
     account's storage trie, then its leaf `RLP(nonce, balance, storageRoot, codeHash)` under
     `keccak(address)`. The account trie, and any storage of more than 4,096 slots, is built from
     ranges cut on a nibble prefix of the keccak keys. The root must equal the anchor's
     `stateRoot`.
   - **Code check**, on another thread. `code-groups-by-code` sorted puts the rows of one code
     together; when loading, each code's reference count is written. Then, one code per worker,
     the bytecode is reassembled from its chunks, must hash to `codeHash`, and re-chunked must give
     exactly the snapshot's groups (a missing leaf stands for a zero chunk). A delegation indicator
     in a `kind 0x01` account rejects.

   If check 2 fails, its error is the one reported. The verifier always waits for the code check
   before deleting its files.

`verifyAndLoad` returns the PBT root, the leaf count and the number of codes; the migrator then
writes the cursor.

## Why passing verify means the snapshot is right

- **Check 1** proves the PBT is exactly the set of leaves derived from the records.
- **The join** proves there is one leaf per preimage and one preimage per leaf.
- **Check 2** proves the leaves' values (nonces, balances, slots, code hashes, delegation targets)
  are the ones the `stateRoot` commits to, with the snapshot's values at the preimages' paths.
- **The code check** ties code chunks and header code sizes to the code hash, which check 2 ties to
  the `stateRoot`.

| Tampering | Caught by | Why |
|---|---|---|
| A slot value changed, `pbtRoot` recomputed to match | check 2 | The join passes, but the storage root, hence the MPT root, changes |
| An extra leaf in the snapshot | join | No request covers its key |
| A leaf missing from the snapshot | join | Its request is never consumed |
| A fake account in both files | check 2 | The rebuilt MPT has one account too many |
| A wrong address in the preimages | join or check 2 | Its PBT key matches no leaf; or `keccak` puts it at the wrong MPT path |
| A code chunk changed | code check | The reassembled code no longer hashes to `codeHash` |
| A wrong `codeSize` | code check | The reassembled code has the wrong length, so the wrong hash |
| One shared code with two sizes | code check | Same request key, different values |
| A code group nobody references | code check | No request for its stem |
| A leaf changed without recomputing `pbtRoot` | check 1 | The computed root differs |
| Malformed file | reader | The canonical rules |

## Keeping it bounded

- **Sort, then walk.** Both sides are sorted into the same order and walked together, instead of
  looking each leaf up in a map of over a billion preimages.
- **External sorts.** Records fill a 64 MiB buffer, which is sorted in parallel and written as a
  run on a background thread while the next one fills. A merge takes at most 512 runs: past that,
  runs are merged 512 at a time into one until 512 are left, so a mainnet-sized sort (thousands of
  runs) makes extra passes.
- **The rank.** Sorting the joined values by rank (8 bytes) rather than by
  `keccak(address) ‖ keccak(slot)` puts them back in MPT order, and re-reading the preimages gives
  the keccak paths. Slots come before their account because the storage root is needed first.
- **Right-edge tries.** Keys arrive in ascending order, so once a key goes right at a node, the left
  subtree is complete: it is hashed, written when loading, and replaced by its hash. Memory is the
  trie's depth (`AscendingCollapseBinaryTrie`, `AscendingCollapsePatriciaTrie`).
- **Parallel subtrees.** A stem, a storage trie, or a range of keys under a nibble prefix depends
  on nothing else except its top node: workers build and hash it, and one thread attaches the top
  nodes in order.

### Writing the PBT

Each stem is written as one entry, and each branch above the stems on its own, at its
**location**, the bit path from the root, with the live trie's encoding. The column therefore reads
back like any stored PBT.

The entries go to sorted files that RocksDB adds to the column as they are: their key ranges do not
overlap and the column is empty, so they land at its last level, with no write-ahead log, memtable
or compaction. Each ingestion stops every write to the database while it runs, so files are added
16 at a time, each batch above the previous one. The global seqno each file gets stays in the
manifest, and an ingestion that would need a memtable flush fails instead of flushing.

A location is `0x00 ‖ path bits ‖ 1`, so stems, which arrive in key order, are in location order
too. A branch arrives after its subtree, but its location sorts before the subtree when its prefix
starts with a 1, between its halves when it has no prefix. The column writer keeps the latest
32 MiB of entries sorted, and writes the lower half when full. A branch whose subtree outgrew that
window, and the root (whose location is empty), come back below what was written: they are kept
apart, a few hundred at most, and written in the same transaction as the cursor.

```text
stems, branches ──► window (sorted, 32 MiB) ──► sorted files ──► added to the column once accepted
                        └─ below what was written ──► written with the cursor, last
```

## How convert works

It streams the preimages, reads each account and slot from the anchor state on one thread (the
state view need not be thread-safe), derives the header and storage leaves in parallel, and sorts
them by PBT key on disk. Codes are widely shared, so each contract only records
`codeHash → address`, and each distinct code is read once and chunked in parallel. The sorted
leaves are then written and the PBT root hashed in one pass, each stem hashed in parallel. The CLI
runs verify on the result.

## Resources

| Piece | Bound |
|---|---|
| Each sort | two 64 MiB buffers (one filling, one being written) + up to 512 open files while merging |
| Between pipeline stages | 4 × threads lists of about 1024 entries (accounts, slots, stems) |
| Snapshot reader / writer | one record |
| Tries | their right edge only |
| Column writer | about 90 MiB of heap: the 32 MiB window and two 16 MiB halves of it being written, up to three batches of at most 8 MiB or 100,000 nodes, and the nodes written last (a few MB; the load fails past 64 MiB) |
| Check 2 | one account per unit, or a range of keys (a few thousand) |
| Code check | one code per thread, ≤ 1 MiB |

Disk: the sorts write temporary files under `<data-path>/pbt-migration`, on the same disk as the
database. They are deleted at the end, and the migrator empties the directory before each
bootstrap attempt. Loading also needs room for the column: its files wait in the work directory,
then are linked into the database, not copied. Sorted records carry a rank, never a 32-byte keccak;
run files store each key as the part that differs from the previous one; convert sorts each
distinct code's chunks once.

In the node, the parallel work runs on the migrator's own pool (`--Xpbt-migration-threads`, half
the cores by default), as do the binary roots of the rolls that follow; the CLI uses the common
pool. Attaching stems, writing the snapshot and reading the preimages stay sequential. When
loading, sorting the window and writing the files run on two background threads.

## Code map

| Package | Class | Role |
|---|---|---|
| `artifact` | `Eip8347TypedSnapshotCodec` | Snapshot records, their rules, and the leaves (`Leaf`) they derive |
| | `Eip8347SnapshotReader` / `Eip8347SnapshotWriter` | Read / write a snapshot, one record at a time, enforcing canonical order |
| | `Eip8347PreimageFile` | Read / write the preimage file |
| | `Eip8347ArtifactVerificationException` | Every rejection (CLI exit code 1) |
| `pipeline` | `Eip8347ExternalSorter` | Sort on disk: sorted runs, then merges of up to 512 runs |
| | `Eip8347Pipelines` | Runs stages on Besu's `services:pipeline` |
| `convert` | `Eip8347SnapshotGenerator` | Convert, from a read-only view of the anchor state (`StateSource`) |
| `verify` | `Eip8347DualCheckVerifier` | Verify: runs the steps above |
| | `Eip8347MptRebuild` | Check 2: rebuilds the MPT, storage tries and account ranges in parallel |
| | `Eip8347AnchorJoin` | Steps 1–2: requests, and the join against headers and storage |
| | `Eip8347CodeCheck` | Code: requests against code groups, then each code's reassembly |

In the parent package: `PbtMigrator` (bootstrap and chain following), `SnapshotColumnWriter` (the
bulk load into the binary column through sorted files), `PbtColumnOwnership` (migrator / chain
handoff) and `BalTrieLogs` (a block's BAL as a roll-forward trie log). The sorted files are a
storage feature: `SegmentedKeyValueStorage.createSortedFileWriter` and `ingestSortedFiles`, which
RocksDB implements with `SstFileWriter` and `IngestExternalFile`.

## CLI

```bash
besu storage pbt verify  --snapshot snap.bin --preimages pre.bin --anchor <hash|number>
besu storage pbt convert --preimages pre.bin --snapshot snap.bin --anchor <hash|number>
besu storage pbt convert --preimages-out pre.bin --snapshot snap.bin --anchor 0   # from genesis
```

Exit code: **0** accepted, **1** rejected, **2** any other failure. `verify` writes nothing.

How much of the node the migrator takes, so that block import keeps up:

| Option | Default | |
|---|---|---|
| `--Xpbt-migration-threads` | half the cores | threads of the snapshot load and of the binary roots |
| `--Xpbt-migration-blocks-per-roll` | 128 | most blocks applied in one roll |
| `--Xpbt-migration-roll-pause` | 0 ms | wait after each roll while catching up |
| `--Xpbt-migration-sort-buffer` | 64 MiB | heap of each external sort of the load |

Its writes are low priority: RocksDB slows them down first when compactions fall behind.

## Spec notes

- **Order.** Artifacts are compared as unsigned bytes, as the spec requires. The PBT library
  compares keys numerically (tuweni `Bytes.compareTo`); both agree because key length is fixed per
  zone.
- **1 MiB code size limit.** Not a spec rule. It stops a hostile header (`codeSize` allows ~4 GiB)
  from making the verifier allocate that much.
- **Converter step 2** (preimages match the MPT leaves exactly) is not a separate scan. A preimage
  with no account or with a zero slot rejects during convert, and a missing preimage is caught by
  the verify that follows.

## File layouts

Snapshot (`x[≤w]` = one length byte, then the minimal big-endian bytes of `x`, at most `w`):

```text
snapshot           = record * | 0x07 | pbtRoot[32]

headerRecord       = kind[1] | addressHash[32] | nonce[≤8] | balance[≤16] | codeRef
                   | slotCount[1] | (slot[1] | value[≤32]) * slotCount
codeGroup          = 0x03 | group
storageAccount     = 0x04 | addressHash[32]
singleStorageGroup = 0x05 | stemHash[32] | subIndex[1] | value[≤32]
storageGroup       = 0x06 | group                         (more than one leaf)

group              = stemHash[32] | n[1] | (subIndex[1] | value[≤32]) * (n + 1)
```

`kind`: `0x00` no code; `0x01` code, `codeRef = codeHash[32] | codeSize[≤4]`; `0x02` EIP-7702
delegation, `codeRef = target[20]`. An account's storage is one `storageAccount` followed by one
or more storage groups.

Preimages: `address[20] | slotCount[4] | slotKey[32] * slotCount` per account, accounts sorted by
`keccak256(address)`, slots by `keccak256(slotKey)`.
