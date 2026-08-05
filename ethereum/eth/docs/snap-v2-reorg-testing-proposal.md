# Snap/2 reorg testing proposal

## Problem

PR #10957 adds a 900+ line acceptance test that validates snap/2 reorg recovery end-to-end. It works, but it is expensive and fragile because it must hit a **timing window**: the pivot must switch to the canonical fork *while* the throttled world-state download is still running.

That forces:

- Engine API block building (Amsterdam FCU/getPayload/newPayload)
- Aggressive download throttling (`parallelism=1`, `count-per-request=1`)
- ~2000 contract deploys to stretch the download window
- Console log string matching
- Complex post-sync tx-pool workarounds

## What already exists (clean)

| Layer | Test | Covers |
|-------|------|--------|
| Plan | `SnapV2ReorgHealerPlanTest` | Reorg decision matrix, tracker lifecycle |
| Healer | `SnapV2ReorgHealerRecoveryTest` | Full `recoverFromReorg()` with real Bonsai + `SnapTestServing` |
| BAL apply | `SnapV2BlockAccessListApplierReorgTest` | Canonical BAL application after orphaned fork |

These are **deterministic**, fast, and already cover the healing logic in depth.

## Gap

Nothing tests the **production wiring** in `SnapV2WorldDownloadState.finishPivotCatchup()`:

```
startPivotCatchup(newPivot)
  → finishPivotCatchup(oldPivot, newPivot)
    → reorgHealer.recoverFromReorg(...)   // when old pivot is orphaned
    → purgeChildRequestsForAccounts(...)
    → retargetQueuedRequests(...)
    → snapSyncState.setCurrentHeader(newPivot)
```

## Proposal: integration test instead of (or alongside) acceptance test

Add `SnapV2WorldDownloadStateReorgIntegrationTest` (see this branch):

1. Build a reorg with `ReorgBlockchainBuilder` (same as healer tests)
2. Seed local Bonsai state as if sync happened on the orphaned fork
3. Serve canonical state via `SnapTestServing` (no real peers)
4. Create `SnapV2WorldDownloadState` with stale pivot in `SnapSyncProcessState`
5. Call `startPivotCatchup(newPivot)` with a stub `SnapV2PivotCatchupListener`
6. Assert healed state + pivot update — **no timing, no Engine API**

### Suggested test pyramid

```
SnapV2ReorgHealerPlanTest              ← logic / plan
SnapV2ReorgHealerRecoveryTest          ← healer + fetch + Bonsai
SnapV2ReorgQueuePurgeTest              ← queue purge for deleted accounts
SnapV2WorldDownloadStateReorgIntegrationTest  ← wiring (NEW)
SnapV2ReorgRecoveryAcceptanceTest      ← optional smoke e2e (thin or @Tag("slow"))
```

## Recommendation for PR #10957

- **Keep** a minimal acceptance smoke test if desired (head sync + one state check), or defer it.
- **Prefer** the integration test on this branch as the primary regression guard for reorg recovery wiring.
- Optionally extract shared helpers from `SnapV2ReorgHealerRecoveryTest` into a package-private fixture to reduce duplication.

## Run

```bash
./gradlew :ethereum:eth:test --tests SnapV2WorldDownloadStateReorgIntegrationTest
```
