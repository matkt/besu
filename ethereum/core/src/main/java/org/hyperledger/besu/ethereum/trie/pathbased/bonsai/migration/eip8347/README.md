# EIP-8347 migration artifacts (Besu)

Offline **convert** (preimages + anchor MPT state → PBT snapshot) and **dual-check verify**
(PBT snapshot + preimages → accept/reject against an MPT `stateRoot`), per
[EIP-8347](https://eips.ethereum.org/EIPS/eip-8347) with the typed, stem-grouped snapshot of
EIP PR 12379.

Both run in **bounded memory** at mainnet scale: nothing holds the leaf set, a stem index or a
per-leaf bitset. Every step streams, and each change of order goes through an external sort on disk.

## Formats

**Snapshot** (byte-canonical, PBT key order):

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

`x[≤w]` = length byte + minimal big-endian bytes. `kind`: `0x00` no code, `0x01`
`codeHash[32] | codeSize[≤4]`, `0x02` EIP-7702 `target[20]`.

**Preimages**: `address[20] | slotCount[4 BE] | slotKey[32] * slotCount`, records sorted by
`keccak256(address)`, slots by `keccak256(slotKey)`.

## Packages

| Package | Class | Role |
|---|---|---|
| `artifact` | `Eip8347TypedSnapshotCodec` | `HeaderRecord` / `Group`, their invariants, leaf derivation, `≤w` integers |
| | `Eip8347SnapshotReader` | Sequential unit reader; enforces canonical order, no trailing byte |
| | `Eip8347SnapshotWriter` | Canonical writer from sorted leaves; large storage records spill to disk |
| | `Eip8347PreimageFile` | Preimage reader (per slot, or bounded batches) + canonical `write` / `writeGenesis` |
| | `Eip8347SnapshotLeaf` | One PBT leaf `(key, value)` |
| | `Eip8347ArtifactVerificationException` | Any reject → CLI exit 1 |
| `pipeline` | `Eip8347ExternalSorter` | Sorted runs + k-way merge, fan-in ≤ 64, multi-pass |
| | `Eip8347Pipelines` | Runs stages on Besu `services:pipeline` (bounded pipes, parallel stages) |
| `convert` | `Eip8347SnapshotGenerator` | Convert pipeline; sorted leaves → snapshot + PBT root |
| | `Eip8347StateSource` | Anchor state view: `of(WorldState)` in production, stubbed in tests |
| `verify` | `Eip8347DualCheckVerifier` | Verify orchestration + MPT rebuild |
| | `Eip8347AnchorJoin` | Preimages ⋈ header/storage records |
| | `Eip8347CodeCheck` | Header code refs ⋈ CODE_ZONE, per-code reassembly check |

## Convert

```text
preimage batches (≤1024 slots) → read anchor state (1 thread) → derive leaves (N threads)
  → external sort → snapshot writer + AscendingCollapseBinaryTrie  → pbtRoot
```

State reads stay on one thread (`StateSource` need not be thread-safe). The CLI then runs the full
dual-check on the result.

## Verify

1. **Requests.** Each preimage account and slot becomes a request keyed by its PBT position
   (`ACCOUNT_ZONE|key_hash(address32)` or the slot's leaf key), valued by its MPT path, and is
   externally sorted. Hashing runs in parallel.
2. **One snapshot pass** (pipeline: read → PBT insert → join):
   - PBT: every derived leaf goes into `AscendingCollapseBinaryTrie`; root ≟ `pbtRoot`.
   - Header and storage records are merge-joined with the sorted requests. An entry with no request
     is a **surplus** leaf, and a request with no entry is a **missing** leaf. Both reject, which
     covers the spec's "every record and entry matches a preimage" rule without a bitset.
   - Matched values are re-emitted keyed by MPT path; `kind 0x01` headers emit code requests.
   - Code groups are merge-joined with the sorted code requests. An unreferenced group rejects,
     and two `codeSize` values for one `code_hash` reject.
3. **Code.** Each `code_hash` is reassembled from its groups, keccak-checked, rejected if it is a
   delegation indicator, and re-chunked. This runs in parallel, one code per item.
4. **MPT.** Matched values, sorted by `keccak(address) | tag | keccak(slot)`, stream into
   `AscendingCollapsePatriciaTrie` (one storage trie at a time); root ≟ `stateRoot`.

Temp files go next to the snapshot (`eip8347-verify-*`, `eip8347-spill-*`,
`eip8347-storage-record-*.tmp`) and are deleted at the end. Disk usage is of the order of the
snapshot size. `verify(..., sortBufferBytes)` and `generate(..., sortBufferBytes)` take an explicit
sort buffer; tests pass `1` to force multi-pass merges.

Stages run concurrently, so a snapshot with several faults reports whichever is detected first
(e.g. an out-of-order header may surface as a missing preimage leaf). Every fault still rejects.

## Memory

| Piece | Bound |
|---|---|
| Each external sort | `DEFAULT_BUFFER_BYTES` (64 MiB) + 64 open run readers |
| Pipes between stages | 256 items (preimage batches ≤ 1024 slots; codes: 16 in flight) |
| Snapshot reader / writer | one unit; writer's storage record > 8 MiB spills to disk |
| PBT / MPT tries | O(depth) |
| Code check | one code per worker, `codeSize ≤ Eip8347CodeCheck.MAX_CODE_SIZE` (1 MiB, DoS guard) |

## Spec notes

- **Order.** Artifacts are compared unsigned byte-lexicographically (`Arrays.compareUnsigned`), as
  the spec requires. The PBT library uses tuweni `Bytes.compareTo` (numeric); both agree here
  because key length is fixed per zone.
- **`MAX_CODE_SIZE` (1 MiB) is not a spec rule.** It is a DoS guard: `codeSize[≤4]` allows ~4 GiB,
  which a hostile header could use to make the verifier allocate that much during reassembly.
- **Converter step 2** (preimages ⇔ MPT leaves exact match) is not a separate scan: a preimage with
  no account or a zero-valued slot rejects during convert, and a missing preimage surfaces in the
  dual-check the CLI runs right after.

## CLI

```bash
besu storage pbt verify  --snapshot snap.bin --preimages pre.bin --anchor <hash|number>
besu storage pbt convert --preimages pre.bin --snapshot snap.bin --anchor <hash|number>
besu storage pbt convert --preimages-out pre.bin --snapshot snap.bin --anchor 0   # from genesis
```

Exit: **0** accept, **1** `Eip8347ArtifactVerificationException`, **2** other failure.

## Tests

| Test | Covers |
|---|---|
| `verify/Eip8347DualCheckVerifierTest` | Accept paths (EOA, contract, delegation, shared code, multi-group code with multi-pass sorts); rejects for roots, order, counts, zero values, surplus / missing preimages, surplus header slot, split storage record, code chunks and size |
| `convert/Eip8347SnapshotGeneratorTest` | Generate + dual-check round trips, tiny sort buffer, missing account, zero-valued slot |
| `artifact/Eip8347PreimageFileTest` | `writeGenesis` ordering and zero-slot skip |
| `pipeline/Eip8347ExternalSorterTest` | In-memory and multi-pass merge order, temp-file cleanup |
