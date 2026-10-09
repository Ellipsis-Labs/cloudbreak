# Atomic Phoenix account snapshots

`getPhoenixAccounts` returns the complete state at the end of one confirmed Solana slot. It combines the configured Phoenix, Ember, Flight and optional TWAP program accounts with explicitly configured global/exchange vault token accounts. All groups use the same database snapshot and confirmed checkpoint. Individual unchanged accounts can have older write slots; they are still the latest account versions at this boundary.

## Configuration and rollout

Apply the new Cloudbreak migration before enabling Phoenix snapshots or exact-account indexing. With both extensions disabled, the upgraded API/indexer also support the legacy schema. It adds an exact-account filter column and a single checkpoint row; it does not reset or delete account data. Enable checkpoint publication on the indexer with `[phoenix-accounts] enabled = true`, and then enable the API method after a complete checkpoint exists.

Set identical program filters and exact vault keys on both the indexer and snapshot loader:

```toml
[programs]
include = ["<phoenix-mainnet>", "<phoenix-beta>", "<ember>", "<flight>"]
accounts = ["<global-vault-token-account>", "<other-required-vault>"]
```

Exact keys are retained regardless of their owner, avoiding indexing the entire SPL Token program. When adding keys or programs to an existing filtered database, reimport/backfill them before enabling this API: changing a filter cannot recover unchanged accounts that were never indexed. Normal startup snapshot loading can supply the initial data. The API refuses requests until snapshot loading and gap repair are healthy and a complete live confirmed block checkpoint is available.

On the API:

```toml
[phoenix-accounts]
enabled = true
program-ids = ["<phoenix-mainnet>", "<phoenix-beta>", "<ember>"]
additional-program-ids = ["<flight>"]
vault-accounts = ["<global-vault-token-account>", "<other-required-vault>"]
twap-program-ids = ["<twap-program>"]
include-twap = true
```

Use actual deployment IDs for every required program, including ancillary Phoenix programs such as flicker, manticore, parrot and adl-queue. No deployment IDs or vault addresses are inferred. For optional Flicker/TWAP coverage, place its program ID only in `twap-program-ids`, not in the always included lists. Configure all required vault keys explicitly; on-chain discovery is not implemented. `include-twap` controls the default inclusion of `twap-program-ids`. A requester may enable that configured list with `includeTwap: true`, provided the indexer covers all its programs. A requester can also explicitly add Flicker through `additionalProgramIds`; the TWAP option is a convenience, not an exclusion rule for explicitly requested programs.

## Request and response

```sh
curl http://localhost:8899 -H 'Content-Type: application/json' -d \
  '{"jsonrpc":"2.0","id":1,"method":"getPhoenixAccounts","params":[{"encoding":"base64+wincode+zstd","includeTwap":false,"additionalProgramIds":["<ember-program-id>","<flight-program-id>","<adl-queue-program-id>"]}]}'
```

`additionalProgramIds` is an optional array of up to 64 public keys, such as Ember, Flight, Parrot, ADL Queue, Manticore or Flicker. These are unioned with the server's base/default program selection, deduplicated and sorted. Every selected program must be fully covered by the indexer's owner filter; an exact-account inclusion does not qualify. Programs outside the indexer coverage produce a secondary-index exclusion error rather than a partial response. No additional RPC lookups or per-program transactions are used: every selected program and the configured vaults are read together at the same checkpoint. Empty requested program groups are retained.

The optional `minContextSlot` rejects a checkpoint older than the requested minimum. There is no processed, finalized, historical or intermediate-transaction read mode.

The result contains:

- `context`: `slot`, `slotIndex`, `transactionCount`, `blockhash`, and `position: "endOfSlot"`.
- `encoding`: `base64+wincode+zstd`.
- `accountCount`, `compressedBytes`, and `data` (base64 of the compressed binary).

`slotIndex` is the last transaction index in the complete block (`transactionCount - 1`), or null for an empty block. It describes the block boundary, not the last transaction that updated each program. This is sufficient for the requested end-of-confirmed-slot consistency; it does not provide intra-slot account history.

## Binary format and client use

Base64-decode `data`, then call `cloudbreak_core::modules::account_snapshot::decode_snapshot`. The zstd level-3 frame contains a wincode tuple:

```text
([u8; 8] magic = CBPHX001, u16 version = 1, AccountSnapshot)
AccountSnapshot { slot, slot_index, transaction_count, blockhash, programs }
ProgramAccounts { program_id: [u8; 32], accounts: Vec<SnapshotAccount> }
SnapshotAccount { pubkey, owner, lamports, executable, rent_epoch, data }
```

Programs and accounts are sorted by their raw public keys. Vaults are grouped under their actual SPL Token program owner. Empty requested program groups are retained. The compressed payload also carries the checkpoint metadata, so consumers can check it against the JSON context.

This uses Phoenix's wincode + zstd strategy, with a distinct versioned schema preserving account metadata and owner groups. It is **not** a `PHXSNAP1` Phoenix state-manager snapshot. A Phoenix consumer must reconstruct its state from these account groups, then run its existing GTI/ATB and vault/global-config invariants. Cloudbreak checks required vault presence and token-account validity; it does not interpret Phoenix layouts or assert that configured vault keys match the on-chain global configuration. Build those checks in the consuming validation tool.

Base64 adds about one third to the compressed binary size. An 8.3 MB compressed payload becomes approximately 11.1 MB before small JSON overhead; actual size differs from Phoenix's snapshot because this format contains more accounts and metadata. Measure `compressedBytes` and actual HTTP response bytes before estimating savings.

## Consistency and operational limits

The indexer publishes its checkpoint only after all account chunks and closure masks have been written. Confirmed slot and checkpoint publication share a transaction. Failed writes stop publication and mark the service unhealthy. Synthetic gap-repair blocks do not fabricate transaction indices. Requests also refuse checkpoints behind the finalized cleanup boundary.

The API reads checkpoint, health and accounts in one read-only repeatable-read transaction. Concurrent ingestion and cleanup cannot mix account versions from different database snapshots. It excludes rows newer than the selected slot and closed accounts. One snapshot request per API process collects and compresses at a time, with a 2 GiB uncompressed bound; replicas can serve independent requests. Large reads retain PostgreSQL row versions until their transaction completes, so monitor query time, memory and vacuum pressure.

Overlapping requests with identical normalized program selections share one database read and compression job through a per-API worker, a bounded request channel and oneshot response fanout. Compression and base64 encoding run on blocking workers. The compressed/base64 payload is shared through an `Arc`; each HTTP response still serializes its own JSON envelope and transmits its own bytes. Caller disconnects do not cancel a build needed by other consumers. Build errors fan out to waiting callers, and the next request can retry.

Each caller checks its own `minContextSlot` against the shared result. If that checkpoint is too old, the caller receives `MinContextSlotNotReached`; coalescing does not silently weaken its minimum. Different program selections never share payloads. Duplicate IDs and different input ordering share work when their final program sets are identical. After fanout, the completed result is dropped; requests arriving later reread the current checkpoint. Coalescing applies within one API instance, not across replicas. This does not add leader election, blue/green routing or deployment automation. Measure CPU/database load before increasing polling frequency.

## Compatibility and rollback

Existing RPC wire formats and legacy configuration defaults are preserved. The indexer checkpoint section defaults to disabled; ordinary slot publication keeps its original parameters and does not touch the extension table. Filter metadata reads support both schemas, and legacy filter writes do not require the new column.

The migration is additive. Legacy SQL readers/writers can access the migrated schema, but an older indexer does not maintain exact vault keys or atomic checkpoints; mixed-version operation of the new endpoint is unsupported. Upgrade and enable the producer before exposing the new endpoint.

For rollback, stop serving the new endpoint and stop checkpoint publication. Remove exact-account filters, restart the indexer so its published filter metadata is cleared, and only then roll back the migration if needed. The down migration refuses to remove nonempty exact-account metadata. Existing account rows are retained. Baseline API/indexer operation supports the rolled-back schema; remove Phoenix-specific configuration before running older binaries.

The generic account-write/slot-publication failure guards remain active even when the endpoint is disabled: failing to persist a complete slot marks the node unhealthy and stops publication/cleanup. This intentional safety behavior is separate from the opt-in checkpoint extension.
