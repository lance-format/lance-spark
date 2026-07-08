# Spark Structured Streaming

The Lance Spark connector supports writing data to Lance tables from a Spark Structured Streaming
query. Each non-empty micro-batch lands as one Lance append (or overwrite, in Complete output mode),
so the table is queryable between batches and time-travel reads see at most one snapshot per epoch
(empty append epochs issue no transaction — see the Exactly-once semantics section below).

> Streaming writes are supported on all built versions (Spark **3.4+**). On Spark 3.4,
> `StreamingWrite.useCommitCoordinator` is absent from the interface, so it is inert there; Lance
> commits each epoch atomically via `CommitBuilder`, so the commit coordinator is immaterial on
> every version.

## Quick start

```python
(spark.readStream
    .schema(schema)
    .parquet("/path/to/source")
    .writeStream
    .format("lance")
    .option("streamingQueryId", "my-query-v1")    # required, see below
    .option("checkpointLocation", "/path/to/checkpoint")
    .trigger(availableNow=True)
    .toTable("lance_ns.default.events"))
```

The target table must already exist; the streaming sink does not auto-create. Create it first with
plain SQL DDL:

```sql
CREATE TABLE lance_ns.default.events (id BIGINT, name STRING);
```

## Required option: `streamingQueryId`

`streamingQueryId` is the **idempotency key** for the query. It must be:

- **Globally unique** across all concurrent streaming queries that write to the same Lance
  table. Two queries sharing a `streamingQueryId` will dedupe each other's epochs and produce
  incorrect results.
- **Stable across restarts** — picking a different value on restart loses replay protection for
  the prior epoch.

The connector fails fast at query start if the option is absent.

## Output modes

| Spark mode | Lance behavior |
|---|---|
| `append` (default) | Each epoch issues a Lance `Append` operation. |
| `complete` | Each epoch issues a Lance `Overwrite` (replacing all existing rows). Requires the upstream query to be a streaming aggregation. |
| `update` | Update-mode rows are routed through the append path — **delta rows are appended, not merged**. This matches the `SupportsStreamingUpdateAsAppend` contract; native MERGE-style upsert is not implemented. |

## Exactly-once semantics

Each non-empty micro-batch performs **one** Lance transaction stamped with `lance.streaming.queryId`
and `lance.streaming.epochId` in the transaction properties. On replay, the connector scans recent
transaction history (`DatasetDelta.listTransactions`) for an existing `(queryId, epochId)` pair —
finding one means the prior attempt already committed, so the current call is a no-op.

**Empty epochs** in `append` mode are no-ops: no Lance transaction is issued. Spark's checkpoint
advances independently, and replays of empty epochs find no prior commit, see no fragments, and skip
again. In `complete` mode an empty epoch still issues **one** `Overwrite` to truncate the table to
the new (empty) result — skipping it would leave the previous epoch's rows on disk.

### Bounded at-least-once fallback

The scan window covers the last `lance.streaming.dedupe.lookback.versions` versions, so a duplicate
can slip through once **`lance.streaming.dedupe.lookback.versions` or more unrelated commits land
between a crash and the restart** — that many intervening commits push the prior commit out of the
window. The default lookback is 100 versions; users with very high commit churn can raise it up to
10 000.

A duplicate can also slip through if a version inside the lookback window is removed by `VACUUM`
(or other version cleanup) before a replay: the dedupe scan can no longer read that version's
transaction, so it skips dedupe for the epoch and the replay may re-commit. Rather than failing the
query, the sink logs a `WARN` and proceeds (the window self-heals as new versions land). To avoid
this, run `VACUUM`/cleanup with a retention window larger than `lance.streaming.dedupe.lookback.versions`
worth of commits on a table that has an active streaming writer.

## Configuration

| Option | Default | Purpose |
|---|---|---|
| `streamingQueryId` | — | Required. Globally unique idempotency key. |
| `lance.streaming.dedupe.lookback.versions` | 100 | Number of recent versions the dedupe scan reads for an already-committed `(queryId, epochId)` pair. The scan runs on **every** epoch (not only at restart), so this is a per-commit cost. Max 10 000. Raise for high commit churn; lower to reduce per-commit dedupe-scan latency. |

The two transaction-property keys (`lance.streaming.queryId`, `lance.streaming.epochId`) are part
of the stability contract — external tooling can rely on them when inspecting Lance transaction
history.

## Limitations

- **CTAS / `REPLACE TABLE`** is rejected. Stage commits commit exactly once, which is structurally
  incompatible with the per-epoch streaming cadence.
- **Streaming reads** (`MicroBatchStream`) are not yet supported — only writes. Tracked
  separately.
- **Row-level UPDATE/DELETE** via Spark's position-delta API is not supported in streaming.
  Append-style writes only.
- **Blob v2 source-context / copy-through** is not applied on the streaming path. Streaming writes
  store blob data directly (which works), but the batch-only optimization that copies blob
  references from a source Lance table does not fire for streaming writes.
- **High-frequency writes (`< 30s` trigger) and Spark 4.1 Real-Time Mode** are out of scope for
  this sink — the trigger→fragments/day math below shows why. A MemWAL-backed sink is the intended
  path for those workloads (follow-up).
- The Lance table must exist before the streaming query starts; the sink does not auto-create.

## Trigger cadence, fragment count & compaction

Each non-empty micro-batch advances the Lance manifest by one version and writes at least one
fragment. Manifest size grows linearly with fragment count, and read-path performance degrades
for very large manifests. Choose a trigger cadence with the following fragment-per-day cost in
mind (Spark actually schedules the next batch at `max(triggerInterval, batchCompletionTime)`, so
busy pipelines produce fewer than the nominal count):

| Trigger | Fragments/run | Fragments/day (typical) | Notes |
|---|---|---|---|
| `RealTime(...)` (Spark 4.1+) | — | — | ❌ Rejected by Spark's `RealTimeModeAllowlist` — only Kafka / ForeachWriter / Console / Memory sinks are allowlisted. Use one of those, or wait for the MemWAL sink. |
| `Continuous(...)`, source is Lance | — | — | ❌ Rejected at planning: Lance does not declare `TableCapability.CONTINUOUS_READ`. |
| `Continuous(...)`, non-Lance source (e.g. Kafka) | — | — | ⚠️ Passes source-side validation and reaches `Write.toStreaming()`, but per-epoch commit cadence is far too high for a Lance sink — expect catastrophic manifest growth. **Not recommended.** |
| `ProcessingTime("10s")` | 1 per epoch | ~8,640 | ❌ Not recommended for this sink; wait for the MemWAL sink. |
| `ProcessingTime("30s")` | 1 per epoch | ~2,880 | ⚠️ Borderline; requires frequent `OPTIMIZE`. |
| `ProcessingTime("1min")` | 1 per epoch | ~1,440 | ✅ Comparable to a daily batch job. |
| `ProcessingTime("5min")` | 1 per epoch | ~288 | ✅ Comfortable. |
| `AvailableNow` | 1 per run | Depends on run cadence | ✅ Ideal (batch-style backfill). |

### `foreachBatch` is not an escape hatch

`foreachBatch` invokes a batch write per epoch and produces the same manifest growth this sink
does. A user willing to buffer records across epochs and hand-roll `(queryId, epochId)`
idempotency in `foreachBatch` could reduce the per-epoch fragment cost, but the logic is
error-prone. The MemWAL follow-up is the intended path for high-frequency writes.

### OPTIMIZE cadence

For any long-running streaming query, schedule `OPTIMIZE` on the target table at a cadence
appropriate to your commit volume — for example, every 1000 epochs at a 5-minute trigger, or
daily at a 1-minute trigger.

