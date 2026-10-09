# Atomic Phoenix account snapshots

`getPhoenixAccounts` returns the complete state at the end of one confirmed Solana slot. It combines the configured Phoenix, Ember, Flight and optional TWAP program accounts with explicitly configured global/exchange vault token accounts and optionally requested token holders selected by mint. All groups use the same database snapshot and confirmed checkpoint. Individual unchanged accounts can have older write slots; they are still the latest account versions at this boundary.

## Configuration and rollout

Apply the new Cloudbreak migration before enabling Phoenix snapshots or exact-account indexing. With both extensions disabled, the upgraded API/indexer also support the legacy schema. The additive migrations add exact-account and token-mint filter metadata plus a single checkpoint row; it does not reset or delete account data. Enable checkpoint publication on the indexer with `[phoenix-accounts] enabled = true`, and then enable the API method after a complete checkpoint exists.

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

## Token mint selection

`TokenMintFilter` identifies a mint **and its token program**. SPL Token is the default; Token-2022 is supported explicitly. Mint accounts and multisigs do not match a holder filter. Initialized and frozen holders are included; pruning zero balances or frozen holders belongs to the future mint-specific endpoint.

On the indexer, enable the owner map at the top level, then add mint filters alongside your Phoenix owner selection. Use the same `[programs]` selection in the snapshot loader:

```toml
accounts-owner-map-enabled = true

[programs]
include = ["<phoenix-mainnet>", "<phoenix-beta>", "<ember>", "<flight>"]
accounts = ["<global-vault-token-account>"]
token-mint-filters = [
  { mint = "<ember-wrapped-usdc-mint>", token-program = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA" },
  { mint = "<token-2022-mint>", token-program = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb" },
]
```

Selection is additive: full program owners, exact keys, **or** matching token holders. Keep token programs out of `include` when you only want selected mints. An empty `include` with an empty `exclude` still means a full index, preserving the existing configuration semantics. Mint selection does not imply coverage of every account owned by a token program. Exact mint keys also do not imply coverage of its holders. Add the mint account itself to `accounts` if you need its metadata for ordinary token RPCs such as UI balance/decimals parsing.

Apply `m20261009_000001_token_mint_filters` before enabling these filters. The indexer validates token-program IDs and requires `accounts-owner-map-enabled = true` for partial token-program coverage, so a same-owner account reinitialized for an unselected mint is masked rather than leaving an old balance in the database. The map tracks each included account's latest slot; older gap repairs cannot discard tracking needed for a subsequent closure. Existing generated `token_mint` columns and their token-mint indexes are reused; enable `idx_accounts_token_mint` and `idx_snapshot_accounts_token_mint` in the existing migration/snapshot index settings.

Changing coverage requires a snapshot reimport/backfill before serving the new selection. An RPC request only selects already indexed data; it never changes subscriptions or discovers missing unchanged holders. API defaults can optionally add `token-mint-filters` under `[phoenix-accounts]`; requester filters are unioned with those defaults. Leaving defaults empty allows each requester to select only the mints it needs.

The current indexer still receives complete confirmed blocks from Yellowstone and filters locally before storage. These filters reduce stored token data and API response bytes; they do **not** reduce upstream block-stream bandwidth. The reusable `TokenMintFilter::subscription_filter()` builds an owner + mint memcmp + token-account-state discovery filter for a future account-stream consumer. A filtered consumer must also subscribe to exact known holder keys to capture closures and mint/owner changes that stop matching discovery, and retain a complete confirmed-slot barrier before publication. Wiring that stream or adding the dedicated mint-only RPC is outside this change.

## Request and response

```sh
curl http://localhost:8899 -H 'Content-Type: application/json' -d \
  '{"jsonrpc":"2.0","id":1,"method":"getPhoenixAccounts","params":[{"encoding":"base64+wincode+zstd","includeTwap":false,"additionalProgramIds":["<ember-program-id>","<flight-program-id>","<adl-queue-program-id>"],"tokenMintFilters":[{"mint":"<ember-wrapped-usdc-mint>","tokenProgram":"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"}]}]}'
```

`additionalProgramIds` is an optional array of up to 64 public keys, such as Ember, Flight, Parrot, ADL Queue, Manticore or Flicker. These are unioned with the server's base/default program selection, deduplicated and sorted. Every selected program must be fully covered by the indexer's owner filter; an exact-account inclusion does not qualify. Programs outside the indexer coverage produce a secondary-index exclusion error rather than a partial response. No additional RPC lookups or per-program transactions are used: every selected program and the configured vaults are read together at the same checkpoint. Empty requested program groups are retained.

`tokenMintFilters` accepts up to 64 `{mint, tokenProgram}` selections (`tokenProgram` defaults to SPL Token). Each must have an identical indexed mint/program filter or full token-program coverage. Unknown coverage is rejected rather than returned as a partial snapshot. Requested holders and required vaults are grouped under their actual token program, deduplicated by public key, and read with Phoenix accounts in the same transaction. Candidate keys are found using owner, exact-key and mint indexes; **all versions of each candidate key** participate in latest-version selection before its current mint/owner is checked. Closed or reinitialized accounts cannot revive an older matching version.

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

Overlapping requests with identical normalized program and token-mint selections share one database read and compression job through a per-API worker, a bounded request channel and oneshot response fanout. Compression and base64 encoding run on blocking workers. The compressed/base64 payload is shared through an `Arc`; each HTTP response still serializes its own JSON envelope and transmits its own bytes. Caller disconnects do not cancel a build needed by other consumers. Build errors fan out to waiting callers, and the next request can retry.

Each caller checks its own `minContextSlot` against the shared result. If that checkpoint is too old, the caller receives `MinContextSlotNotReached`; coalescing does not silently weaken its minimum. Different program or mint selections never share payloads. Duplicate IDs and different input ordering share work when their final program sets are identical. After fanout, the completed result is dropped; requests arriving later reread the current checkpoint. Coalescing applies within one API instance, not across replicas. This does not add leader election, blue/green routing or deployment automation. Measure CPU/database load before increasing polling frequency.

## Compatibility and rollback

Existing RPC wire formats and legacy configuration defaults are preserved. The indexer checkpoint section defaults to disabled; ordinary slot publication keeps its original parameters and does not touch the extension table. Filter metadata reads support both schemas, and legacy filter writes do not require the new column.

The migration is additive. Legacy SQL readers/writers can access the migrated schema, but an older indexer does not maintain exact vault keys or atomic checkpoints; mixed-version operation of the new endpoint is unsupported. Upgrade and enable the producer before exposing the new endpoint.

For rollback, stop serving the new endpoint and stop checkpoint publication. Remove exact-account and token-mint filters, restart the indexer so its published filter metadata is cleared, and only then roll back the migration if needed. The down migrations refuse to remove active exact-account or token-mint metadata. Existing account rows are retained. Baseline API/indexer operation supports the rolled-back schema; remove Phoenix-specific configuration before running older binaries.

The generic account-write/slot-publication failure guards remain active even when the endpoint is disabled: failing to persist a complete slot marks the node unhealthy and stops publication/cleanup. This intentional safety behavior is separate from the opt-in checkpoint extension.

## Maintaining the upstream fork

The upstream repository is `https://github.com/solana-rpc/cloudbreak.git`; the deployment fork is `Ellipsis-Labs/cloudbreak`. Keep them as separate `upstream` and `origin` remotes. Merge upstream updates into a clean feature/integration checkout and validate them before pushing. Preserve published commit history; upstream synchronization does not require rebasing or force-pushing this PR.

Most extension code lives in dedicated files: `api/src/methods/phoenix_accounts.rs`, `core/src/modules/account_snapshot.rs`, `core/src/modules/token_mint_filter.rs`, and `index/src/modules/account_checkpoint.rs`. The existing RPC dispatcher has one additional method arm. Config additions default to disabled/empty, migrations are additive, and legacy metadata schemas remain supported when their extensions are disabled. There are no Phoenix SDK or program-crate dependencies.

Shared code still needs care during upstream merges:

- Keep `is_program_selected` as a full-owner coverage check. Data-aware exact-key/mint selection uses `is_account_selected`; partial mint coverage must not enable program-wide RPCs.
- Keep checkpoint SQL in its extension module. Both publishers use `db_queries::slot_statement`, so upstream slot-upsert changes have one SQL implementation to update.
- Preserve the account-write failure checks before publication and cleanup, plus the owner map's latest-slot guards. These prevent incomplete snapshots and stale balances after closure, reinitialization or an older repair.
- Merge migration registration lists while retaining already published migration names and their rollback guards. Do not rewrite applied migrations to resolve an upstream conflict.

For this PR, run these commands from its clean checkout, resolving any merge conflicts before validation:

```sh
git fetch origin
git fetch upstream main
git merge --no-edit origin/main
git merge --no-edit upstream/main
cargo +1.98.1 check --tests -p cloudbreak
cargo +1.98.1 test --lib -p cloudbreak-core -p cloudbreak-api -p cloudbreak-index -p cloudbreak-migration
git push origin HEAD:gally/feat/atomic-phoenix-accounts
```

Also run the ignored PostgreSQL tests against a disposable local database by setting `CLOUDBREAK_TEST_DATABASE_URL` and adding `-- --include-ignored` to the test command. They cover checkpoint publication failure, legacy-schema upgrade/rollback, atomic reads, mint changes and closure tracking. A conflict-free Git merge alone does not verify those behaviors. The fork's current upstream base can be checked with `git merge-base HEAD upstream/main`; feature changes are reviewed with `git diff origin/main...HEAD`.
