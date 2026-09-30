# EIP-8347 migration artifacts

Besu's implementation of the [EIP-8347](https://eips.ethereum.org/EIPS/eip-8347) artifacts, with
the typed, stem-grouped snapshot format of EIP PR 12379.

- [What it is for](#what-it-is-for)
- [The two files](#the-two-files)
- [Importing a snapshot into a node](#importing-a-snapshot-into-a-node)
- [How verify works, step by step](#how-verify-works-step-by-step)
- [A worked example](#a-worked-example)
- [Why passing verify means the snapshot is right](#why-passing-verify-means-the-snapshot-is-right)
- [The techniques that keep it bounded](#the-techniques-that-keep-it-bounded)
- [How convert works](#how-convert-works)
- [Resources](#resources) · [Code map](#code-map) · [CLI](#cli) · [Spec notes](#spec-notes) ·
  [File layouts](#file-layouts) · [Tests](#tests)

## What it is for

At the PBT fork, every node needs the pre-fork state as a PBT (the binary trie of EIP-8297)
instead of an MPT. A node can build it alone by replaying the chain from genesis into its
binary-trie column, which is slow. EIP-8347 lets it start instead from a **PBT snapshot** taken at
a finalized pre-fork **anchor** block.

The node trusts nothing about that snapshot. The one thing it can trust is the anchor's
`stateRoot`: it is in the block header, and consensus validated it. The **dual check** proves that
the snapshot is exactly the state that `stateRoot` commits to.

The difficulty is that the two tries index the state differently:

| | MPT (what the header commits) | PBT (what the snapshot holds) |
|---|---|---|
| Account key | `keccak(address)` | `ACCOUNT_ZONE ‖ blake3(address32) ‖ subIndex` |
| Slot key | `keccak(slotKey)`, in the account's storage trie | a header leaf `ACCOUNT_ZONE ‖ blake3(address32) ‖ (64 + slot)` if `slot < 64`, else `STORAGE_ZONE ‖ blake3(address32) ‖ blake3(address32 ‖ slot / 256) ‖ slot % 256` |
| Order | keccak order | BLAKE3 order |

Hashes cannot be inverted, so going from a PBT key to an MPT key needs the **preimages**: the actual
addresses and slot keys.

This package provides both sides:

- **Convert**: anchor state (MPT) + preimages → PBT snapshot.
- **Verify** (the *dual check*): PBT snapshot + preimages → accept or reject against the anchor's
  `stateRoot`.

Both run offline from the CLI. In the node, the PBT migrator bootstraps with `verifyAndLoad`,
which verifies the snapshot and writes the PBT into the binary-trie column in the same pass.

## The two files

### Preimages

```text
per account, sorted by keccak256(address):
  address[20] | slotCount[4] | slotKey[32] × slotCount      (slots sorted by keccak256(slotKey))
```

This is the exact list of accounts and non-zero slots of the anchor state, **in MPT order**. That
order matters later (see [the rank](#the-rank)).

### Snapshot

```text
pbtRoot[32]                                      the root the snapshot claims
headerCount[8]  | headerRecord × headerCount     ACCOUNT_ZONE
codeCount[8]    | group × codeCount              CODE_ZONE
storageCount[8] | storageRecord × storageCount   STORAGE_ZONE
```

- A **header record** is one account's whole header stem: `addressHash`, nonce, balance, `kind`,
  and storage slots 0–63. `kind` is `0x00` (no code), `0x01` (code: `codeHash`, `codeSize`) or
  `0x02` (EIP-7702 delegation: 20-byte `target`).
- A **group** is one stem's leaves: `stemHash` then `(subIndex, value)` pairs. Code groups hold code
  chunks.
- A **storage record** is one account's storage outside the header: `addressHash` and its groups.

The records hold typed fields (nonce, balance, code reference…) rather than raw leaves. The PBT
leaves are **derived** from them deterministically. This makes the file smaller and
**byte-canonical**: one state gives exactly one file. The reader therefore rejects anything
non-canonical:

- records or entries not in strictly ascending order;
- non-minimal integers (a leading zero byte);
- zero values;
- empty accounts (excluded by EIP-7523);
- an invalid `kind`;
- wrong section counts;
- trailing bytes.

The exact layouts are in [File layouts](#file-layouts).

## Importing a snapshot into a node

1. **Configuration.** The node is started with `--Xpbt-snapshot-file`, `--Xpbt-preimages-file` and
   `--Xpbt-snapshot-anchor-block-hash`. They become a `PbtMigrator.SnapshotBootstrap`: the two files, the
   anchor hash, and the work directory `<data-path>/pbt-migration`.
2. **The migrator starts.** `PbtMigrator` ticks every second. While it has no **cursor** (the block
   whose state the binary column holds), it tries to bootstrap.
3. **Waiting for the anchor.** `bootstrapFromSnapshot` waits until the anchor is known, canonical,
   finalized, and before the fork. Until then it retries on the next tick.
4. **Cleaning up.** It empties the binary column and the work directory. With no cursor, whatever
   they hold comes from an interrupted attempt.
5. **Verify and load.** It creates a `ColumnWriter`, a `NodeUpdater` onto the binary
   column. Writes go through `MigrationScopedWorldStateKeyValueStorage`, which only lets trie nodes
   through, never the MPT's flat DB. The writer commits every 100,000 writes. The migrator then
   calls `Eip8347DualCheckVerifier.verifyAndLoad(...)`.
6. **Accepted.** `writer.finish(root, anchorHash)` writes the **cursor, last**, and the migrator's
   cursor becomes the anchor.
7. **Rejected** (`Eip8347ArtifactVerificationException`). The column is emptied and the migrator
   stops: reading the same file again would give the same rejection.
8. **Any other failure** (disk full, I/O…). The column is emptied and the load is retried on the
   next tick.

The migrator holds the binary-column lock (`PbtColumnOwnership`) for the whole load. If the chain
reaches the fork meanwhile, it waits for the load to end ("Waiting for the PBT migrator…").

**Crash safety** rests on one rule: no cursor means the column is not valid. A crash mid-load leaves
no cursor, so on restart the column is emptied and the load starts over.

**After the import**, the migrator follows the chain from the anchor as usual, at most 128 blocks
per step: through trie logs when the node has them, otherwise through each block's BAL (EIP-7928),
stored locally or fetched from eth/71 peers. A BAL records post-values only, so it can only roll
forward. At the fork, the chain claims the column and the migrator retires.

## How verify works, step by step

`run` creates a temporary sub-directory of the work directory and four **external sorts**:

| Sort | Key → value |
|---|---|
| `preimage-requests` | PBT key → rank in the preimage file |
| `mpt-values` | rank → value (a slot value, or an account's fields) |
| `code-group-requests` | code group stem → `codeHash, group, codeSize` |
| `code-groups-by-code` | `codeHash ‖ group` → `codeSize, references, chunks` |

### Step 1: requests (first read of the preimages)

The preimage file is read in batches of at most 1024 slots, **without computing any keccak**: this
pass only needs PBT keys, and step 4 checks the keccak order.

Each entry gets its **rank**, its position in the file, an account's slots first and the account
right after them. Its PBT key is computed in parallel (`blake3(address)` once per account). Each
entry becomes a **request** `PBT key → rank` in `preimage-requests`.

Once sorted, the requests are **in PBT order**, the order of the snapshot.

### Step 2: one pass over the snapshot

```text
reader ──► hash stems (N threads, order kept) ──► attach to the PBT ──► join
```

1. **Reader.** Returns units: a header, a code group, or a storage group. Each unit is **one whole
   stem**. The reader enforces the canonical rules above.
2. **Hash stems**, in parallel. `AscendingCollapseBinaryTrie.prepare` builds the stem's small
   subtree, hashes every leaf and inner node, encodes the nodes to write, and replaces the subtree by
   hash stubs.
3. **Attach**, on one thread, in order. `AscendingCollapseBinaryTrie.insert` attaches the stem's
   top node, the only hash that depends on where the stem lands. When loading, every node is written
   to the binary column at its location.
4. **Join**, against the sorted requests, like merging two sorted lists:
   - **Header:**
     - its account request must be next;
     - each of its slots 0–63 must match a slot request;
     - a `kind 0x01` header requests the code groups its `codeSize` spans;
     - it emits `rank → (nonce, MPT code hash, balance)` into `mpt-values`. The MPT code hash is
       `EMPTY` for no code, `codeHash` for code, and `keccak(0xef0100 ‖ target)` for a delegation.
   - **Storage leaf:** must match its request, and emits `rank → value`.
   - **Code group:**
     - every code request is known by then, because headers come first;
     - the requests are sorted by stem, and the group must match one;
     - identical requests (accounts sharing a code) are folded and **counted**, which gives the
       code's reference count. Two `codeSize` values for one `codeHash` reject;
     - the group, or "no leaf" for a group that has none, is re-emitted under `codeHash ‖ group`.

### Step 3: end of the pass

- The reader must be exactly at the end of the file.
- **Check 1, internal consistency:** the computed PBT root must equal the claimed `pbtRoot`. When
  loading, the root node is written now.
- No request may be left, otherwise a preimage has no leaf.
- `preimage-requests` is deleted: it is no longer needed.

### Step 4: two checks at the same time

**Check 2, consensus anchoring** (second read of the preimages, this time checking the keccak
order):

1. `mpt-values` is sorted by rank, which puts the values back in preimage order, that is MPT
   order.
2. The preimages are read alongside, account by account:
   - each slot's value goes into the account's storage trie under `keccak(slotKey)`;
   - then `RLP(nonce, balance, storageRoot, codeHash)` goes into the account trie under
     `keccak(address)`.
3. The resulting root must equal the anchor's **`stateRoot`**.

**Code check**, on another thread:

1. `code-groups-by-code` is sorted, so the rows of one code come together.
2. When loading, each code's reference count (accounts using it, chunk count) is written: the live
   trie needs it to know when a shared code can be deleted.
3. In parallel, one code per thread:
   - reassemble the bytecode from its chunks (each chunk is one byte counting the PUSH data that
     spills over from the previous chunk, then 31 bytes of code). A chunk beyond `codeSize`
     rejects;
   - reject a delegation indicator (`0xef0100…`) in a `kind 0x01` account;
   - `keccak(bytecode)` must equal `codeHash`;
   - re-chunk the bytecode: the result must be exactly the snapshot's groups, where a missing leaf
     stands for a zero chunk.

If the MPT check fails, its error is the one reported. The verifier always waits for the code check
before deleting its files.

### Step 5: done

`verifyAndLoad` returns the PBT root, the leaf count and the number of codes, then deletes its
temporary directory. The migrator writes the cursor.

## A worked example

Hashes are made up and shortened. Only their order matters.

**Anchor state:**

| Account | Content |
|---|---|
| Alice (EOA) | nonce 1, balance 50, slot `5 = 7`, slot `1000 = 3` |
| Bob (contract) | nonce 0, balance 9, code `0x6001600055` (5 bytes) with hash `H` |

| | keccak (MPT) | blake3 (PBT) |
|---|---|---|
| Alice | `0x8f…` | `0x2d…` |
| Bob | `0x3a…` | `0x71…` |
| slot 5 | `0xc1…` | |
| slot 1000 | `0x44…` | |

In MPT order Bob comes first (`0x3a < 0x8f`); in PBT order Alice does (`0x2d < 0x71`).

**Preimage file** (MPT order):

```text
Bob   | 0 slots
Alice | 2 slots | slot 1000 | slot 5         (1000 first: 0x44 < 0xc1)
```

**PBT keys:**

- Alice's header: `00‖2d`. Slot 5 is below 64, so it is in the header: `00‖2d‖69` (`64 + 5`).
- Slot 1000 goes to the storage zone: `FF‖2d‖blake3(A‖3)‖232` (`1000 = 3 × 256 + 232`).
- Bob's header: `00‖71`. His 5-byte code is one chunk, so one code group:
  `01‖stem(H, group 0)‖0`.

**Snapshot:**

```text
pbtRoot R
headers:  [Alice: nonce 1, balance 50, kind 0, slots {5: 7}]
          [Bob:   nonce 0, balance 9,  kind 1 (H, size 5)]
code:     [group stem(H,0): {0: chunk0}]
storage:  [Alice: one group {232: 3}]
```

**Step 1, ranks and requests.** Ranks in file order, slots before their account:

```text
Bob → 0      Alice.1000 → 1      Alice.5 → 2      Alice → 3
```

Requests sorted by PBT key:

```text
00‖2d     → 3   (Alice)
00‖2d‖69  → 2   (Alice.5: same start, longer, so right after)
00‖71     → 0   (Bob)
FF‖…‖232  → 1   (Alice.1000)
```

**Step 2, the join** walks both lists together:

```text
snapshot                      requests
header Alice (00‖2d)     ─▶   00‖2d → 3      matched  → mpt-values: 3 → (1, EMPTY, 50)
  slot 5 (00‖2d‖69)      ─▶   00‖2d‖69 → 2   matched  → mpt-values: 2 → 7
header Bob (00‖71)       ─▶   00‖71 → 0      matched  → mpt-values: 0 → (0, H, 9)
                                                         and requests group 0 of H (size 5)
code group stem(H,0)          meets Bob's code request → row H‖0 → (5, 1 reference, chunk0)
storage Alice (FF‖…‖232) ─▶   FF‖…‖232 → 1   matched  → mpt-values: 1 → 3
end                           no request left
```

Meanwhile the PBT is hashed, and its root compared with `R` (check 1).

**Step 4, back to MPT order.** Sorted by rank, the values read `0: Bob, 1: 3, 2: 7, 3: Alice`,
which is the preimage order:

```text
preimages       values       action
Bob, 0 slots    0: Bob    →  account Bob at 0x3a…, empty storage root
Alice, 2 slots
  slot 1000     1: 3      →  Alice's storage trie: 0x44… → 3
  slot 5        2: 7      →                        0xc1… → 7
                3: Alice  →  account Alice at 0x8f…, with that storage root
```

The root must equal the anchor's `stateRoot` (check 2). In parallel, the code check reassembles 5
bytes from `chunk0`, checks `keccak = H`, re-chunks, and records that `H` has 1 reference and 1
chunk.

## Why passing verify means the snapshot is right

Every part of the snapshot is tied to the `stateRoot`:

- **Check 1** proves the PBT is exactly the set of leaves derived from the records.
- **The join** proves there is one leaf per preimage and one preimage per leaf.
- **Check 2** proves the leaves' values (nonces, balances, slots, code hashes, delegation targets)
  are the ones the `stateRoot` commits to. The rebuild uses the snapshot's values and the
  preimages' paths.
- **The code check** ties code chunks and header code sizes to the code hash, which check 2 ties to
  the `stateRoot`.

What each kind of tampering runs into:

| Tampering | Caught by | Why |
|---|---|---|
| A slot value changed, `pbtRoot` recomputed to match | check 2 | The join passes, but the storage root, hence the MPT root, changes |
| An extra leaf in the snapshot | join | No request covers its key: "not covered by consensus anchoring" |
| A leaf missing from the snapshot | join | Its request is never consumed: "preimage … has no snapshot leaf" |
| A fake account in both files | check 2 | The rebuilt MPT has one account too many |
| A wrong address in the preimages | join or check 2 | Its PBT key matches no leaf; or, if the snapshot uses the same wrong address, `keccak` puts it at the wrong MPT path |
| A code chunk changed | code check | The reassembled code no longer hashes to `codeHash` |
| A wrong `codeSize` | code check | The reassembled code has the wrong length, so the wrong hash |
| One shared code with two sizes | code check | Same request key, different values: "disagree on codeSize" |
| A code group nobody references | code check | No request for its stem: "not referenced" |
| A leaf changed without recomputing `pbtRoot` | check 1 | The computed root differs |
| Malformed file | reader | The canonical rules |

## The techniques that keep it bounded

### Sort, then walk, instead of looking up

The naive approach is to load every preimage into a hash map and look each leaf up. At mainnet
scale (over a billion entries) that is tens of gigabytes of heap. Instead, both sides are sorted
into the same order and walked together, one entry of each at a time, like checking that two decks
of cards match by sorting both and turning one card of each at a time.

### Sorting more than fits in memory

```text
records ──► [64 MiB buffer] ─sort─► run 1
        ──► [64 MiB buffer] ─sort─► run 2
        ──► …                       run N

merge: open up to 512 runs at once, always take the smallest head
```

Each run is sorted, so the smallest record overall is at the head of some run. Memory is one record
per open run. A full buffer is sorted in parallel and written on a background thread while the next
one fills. With up to 512 runs per merge, a mainnet-sized sort merges in a single pass.

### The rank

After the join, values come out in PBT order, but the MPT needs them in MPT order. Re-sorting them by
`keccak(address) ‖ keccak(slot)` would carry 64 bytes of key per entry. The preimage file is
already in MPT order, so an entry's position in it (8 bytes) is enough: sorting by rank puts the
values back in MPT order, and re-reading the file gives the keccak paths. Slots are numbered before
their account because the storage root has to be known before the account is inserted.

### A trie that only keeps its right edge

Keys arrive in ascending order. Once a key goes right at a node, no later key will go left of it:
the left subtree is **complete**. It is hashed, written when loading, and replaced by its hash.

```text
        root
       /    \
   [hash]    •          only the rightmost path stays in memory
            / \
        [hash] •
              / \
          [hash] ◄ last key
```

Memory is the trie's depth, not its size. The PBT and the rebuilt MPT both work this way
(`AscendingCollapseBinaryTrie`, `AscendingCollapsePatriciaTrie`).

### Hashing stems in parallel

A snapshot unit holds every leaf of a stem (keys equal in all but their last byte). Their subtree
depends on nothing else, except for its top node, whose stored prefix depends on its neighbours.
So the heavy work (hashing leaves and inner nodes, encoding them) runs on many threads, and one
thread attaches the top nodes in order.

### Writing the PBT

Each node is written at its **location**, the bit path from the root, with the same encoding the
live Besu trie uses. The column therefore reads back like any stored PBT. Writes are committed every
100,000 nodes, and the cursor is written last.

## How convert works

It streams the preimages, reads each account and slot from the anchor state (one thread: the state
view need not be thread-safe), derives the PBT leaves in parallel, sorts them by PBT key on disk,
then writes the snapshot and hashes the PBT root in one pass. Leaves emitted twice (a code shared by
several accounts) are merged. The CLI then runs verify on the result.

## Resources

Memory stays bounded whatever the state size:

| Piece | Bound |
|---|---|
| Each sort | two 64 MiB buffers (one filling, one being written) + up to 512 open files while merging |
| Between pipeline stages | 256 items (batches of ≤ 1024 slots) |
| Snapshot reader / writer | one record; a writer's storage record over 8 MiB goes to disk |
| Tries | their right edge only |
| Code check | one code per thread, ≤ 1 MiB |

Disk: the sorts write temporary files under `<data-path>/pbt-migration` (next to `database`, so on
the same disk). They are deleted at the end, and the migrator empties the directory before each
bootstrap attempt. Verify needs about **1.3× the snapshot size** of free space at its peak, convert
about 1.5×. Two things keep this low:

- Sorted records carry a rank (8 bytes), never a 32-byte keccak.
- Sorted files store each key as the part that differs from the previous one. Slots of one
  contract share their first 33 bytes.

These figures are estimates from the record sizes, not mainnet measurements.

Parallel: PBT keys of the preimages, stem hashing, code checks, and the code check alongside the
MPT rebuild. Sequential: attaching stems to the PBT, the MPT rebuild, and database writes.

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
| `verify` | `Eip8347DualCheckVerifier` | Verify: runs the steps above and rebuilds the MPT |
| | `Eip8347AnchorJoin` | Steps 1–2: requests, and the join against headers and storage |
| | `Eip8347CodeCheck` | Code: requests against code groups, then each code's reassembly |

In the parent package: `PbtMigrator` (bootstrap and chain following; it nests the configured
artifacts, `SnapshotBootstrap`, and the bulk writer onto the binary column, `ColumnWriter`),
`PbtColumnOwnership` (migrator / chain handoff) and `BalTrieLogs` (a block's BAL as a roll-forward
trie log).

## CLI

```bash
besu storage pbt verify  --snapshot snap.bin --preimages pre.bin --anchor <hash|number>
besu storage pbt convert --preimages pre.bin --snapshot snap.bin --anchor <hash|number>
besu storage pbt convert --preimages-out pre.bin --snapshot snap.bin --anchor 0   # from genesis
```

Exit code: **0** accepted, **1** rejected, **2** any other failure. `verify` writes nothing.

To bootstrap a node from a snapshot: `--Xpbt-snapshot-file`, `--Xpbt-preimages-file` and
`--Xpbt-snapshot-anchor-block-hash`, set together.

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
pbtRoot[32]
  | headerCount[8]  | headerRecord * headerCount      ACCOUNT_ZONE
  | codeCount[8]    | group * codeCount               CODE_ZONE
  | storageCount[8] | storageRecord * storageCount    STORAGE_ZONE

headerRecord  = addressHash[32] | nonce[≤8] | balance[≤16] | kind[1] | codeRef
              | slotCount[1] | (slot[1] | value[≤32]) * slotCount
storageRecord = addressHash[32] | groupCount[≤8] | group * groupCount
group         = stemHash[32] | n[1] | (subIndex[1] | value[≤32]) * (n + 1)
```

`kind`: `0x00` no code; `0x01` code, `codeRef = codeHash[32] | codeSize[≤4]`; `0x02` EIP-7702
delegation, `codeRef = target[20]`.

Preimages: `address[20] | slotCount[4] | slotKey[32] * slotCount` per account, accounts sorted by
`keccak256(address)`, slots by `keccak256(slotKey)`.

## Tests

`Eip8347Fixture` (test sources) builds a small anchor state and computes, independently of the code
under test, what EIP-8347 derives from it: PBT leaves and root, preimages, MPT root.

| Test | Covers |
|---|---|
| `verify/Eip8347DualCheckVerifierTest` | Accepted snapshots; `verifyAndLoad` output; every kind of rejection (roots, layout, preimage mismatches, code) |
| `convert/Eip8347SnapshotGeneratorTest` | Generated snapshot byte-equal to the fixture's, whatever the sort buffer; convert rejections |
| `artifact/Eip8347SnapshotFormatTest` | Write → read round trip; non-canonical records and leaves |
| `artifact/Eip8347PreimageFileTest` | Genesis preimages; unsorted or truncated files |
| `pipeline/Eip8347ExternalSorterTest` | Sort order, multi-pass merges, on-disk encoding, cleanup |
