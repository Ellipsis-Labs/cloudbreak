//! Atomic, multi-program reads at the end of a published confirmed slot.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, LazyLock, OnceLock};

use base64::{Engine, engine::general_purpose::STANDARD};
use cloudbreak_core::modules::account_snapshot::{
    AccountSnapshot, MAX_SNAPSHOT_BYTES, ProgramAccounts, SnapshotAccount, encode_snapshot,
};
use cloudbreak_core::{AccountSelectorConfig, PhoenixAccountsConfig, PubkeyDef, TokenMintFilter};
use futures::{StreamExt, future::BoxFuture, stream::FuturesUnordered};
use rust_decimal::prelude::ToPrimitive;
use sea_orm::sqlx::{self, Row};
use serde::{Deserialize, Serialize};
use solana_pubkey::Pubkey;
use tokio::sync::{Semaphore, mpsc, oneshot};

use crate::{error::RpcError, http::CloudbreakRpcState, metrics::GpaMetricsData};

// Bound large account collections/compression independently of ordinary RPC requests.
static SNAPSHOT_PERMIT: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(1)));

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize)]
pub enum SnapshotEncoding {
    #[default]
    #[serde(rename = "base64+wincode+zstd")]
    Base64WincodeZstd,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GetPhoenixAccountsConfig {
    #[serde(default)]
    pub encoding: SnapshotEncoding,
    pub min_context_slot: Option<u64>,
    /// Optional shorthand for including configured TWAP program IDs.
    pub include_twap: Option<bool>,
    /// Extra fully indexed programs to include at the same confirmed-slot boundary.
    #[serde(default)]
    pub additional_program_ids: Vec<PubkeyDef>,
    /// Additional already indexed token-holder sets, read at this checkpoint.
    #[serde(default)]
    pub token_mint_filters: Vec<TokenMintFilter>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotContext {
    pub slot: u64,
    pub slot_index: Option<u64>,
    pub transaction_count: u64,
    pub blockhash: String,
    pub position: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PhoenixAccountsResponse {
    pub context: SnapshotContext,
    pub encoding: SnapshotEncoding,
    pub account_count: usize,
    pub compressed_bytes: usize,
    pub data: String,
}

pub fn selected_programs(config: &PhoenixAccountsConfig, include_twap: bool) -> Vec<Pubkey> {
    config
        .program_ids
        .iter()
        .chain(&config.additional_program_ids)
        .chain(config.twap_program_ids.iter().filter(|_| include_twap))
        .map(|key| key.0)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn request_programs(
    config: &PhoenixAccountsConfig,
    include_twap: bool,
    additional: &[PubkeyDef],
    indexer_filter: &cloudbreak_core::AccountSelectorConfig,
) -> Result<Vec<Pubkey>, RpcError> {
    if additional.len() > 64 {
        return Err(RpcError::InvalidParamsWithMessage(
            "At most 64 additionalProgramIds are supported".into(),
        ));
    }
    if include_twap && config.twap_program_ids.is_empty() {
        return Err(RpcError::InvalidParamsWithMessage(
            "No TWAP program IDs are configured; use additionalProgramIds".into(),
        ));
    }
    let programs: Vec<_> = selected_programs(config, include_twap)
        .into_iter()
        .chain(additional.iter().map(|p| p.0))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    for program in &programs {
        if !indexer_filter.is_program_selected(program) {
            return Err(RpcError::KeyExcludedFromSecondaryIndex {
                key: program.to_string(),
            });
        }
    }
    Ok(programs)
}

fn request_mint_filters(
    config: &PhoenixAccountsConfig,
    additional: &[TokenMintFilter],
    indexer_filter: &AccountSelectorConfig,
) -> Result<Vec<TokenMintFilter>, RpcError> {
    if additional.len() > 64 {
        return Err(RpcError::InvalidParamsWithMessage(
            "At most 64 tokenMintFilters are supported".into(),
        ));
    }
    let filters: Vec<_> = config
        .token_mint_filters
        .iter()
        .chain(additional)
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    for filter in &filters {
        filter
            .validate()
            .map_err(|e| RpcError::InvalidParamsWithMessage(e.to_string()))?;
        if !indexer_filter.covers_token_mint(filter) {
            return Err(RpcError::KeyExcludedFromSecondaryIndex {
                key: filter.mint.0.to_string(),
            });
        }
    }
    Ok(filters)
}

#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
struct SnapshotSelection {
    programs: Vec<Pubkey>,
    token_mint_filters: Vec<TokenMintFilter>,
}

impl From<Vec<Pubkey>> for SnapshotSelection {
    fn from(programs: Vec<Pubkey>) -> Self {
        Self {
            programs,
            ..Default::default()
        }
    }
}

pub fn validate_config(
    config: &PhoenixAccountsConfig,
    state: &CloudbreakRpcState,
) -> Result<(), RpcError> {
    if config.program_ids.is_empty() || config.vault_accounts.is_empty() {
        return Err(RpcError::InvalidParamsWithMessage(
            "phoenix-accounts requires program-ids and vault-accounts".into(),
        ));
    }
    if config.include_twap && config.twap_program_ids.is_empty() {
        return Err(RpcError::InvalidParamsWithMessage(
            "include-twap requires twap-program-ids".into(),
        ));
    }
    if config.twap_program_ids.iter().any(|twap| {
        config
            .program_ids
            .iter()
            .chain(&config.additional_program_ids)
            .any(|p| p.0 == twap.0)
    }) {
        return Err(RpcError::InvalidParamsWithMessage(
            "TWAP programs must appear only in twap-program-ids".into(),
        ));
    }
    for program in selected_programs(config, config.include_twap) {
        if !state.indexer_filter.is_program_selected(&program) {
            return Err(RpcError::KeyExcludedFromSecondaryIndex {
                key: program.to_string(),
            });
        }
    }
    request_mint_filters(config, &[], &state.indexer_filter)?;
    for key in &config.vault_accounts {
        if !state.indexer_filter.accounts.iter().any(|p| p.0 == key.0) {
            return Err(RpcError::InvalidParamsWithMessage(format!(
                "Vault {} must be explicitly indexed in [programs].accounts",
                key.0
            )));
        }
    }
    Ok(())
}

const CHECKPOINT_SQL: &str = "SELECT c.slot, c.transaction_count, c.blockhash FROM atomic_account_checkpoint c JOIN slots s ON s.commitment = 1 AND s.slot = c.slot JOIN service_health h ON h.id = 1 WHERE c.id = 1 AND s.health AND h.healthy AND NOT EXISTS (SELECT 1 FROM slots f WHERE f.commitment = 2 AND f.slot > c.slot)";

// Find eligible keys with the existing owner/key/mint indexes, then choose their latest
// version without filtering its owner or mint: closures/reinitialization must mask old balances.
const ACCOUNTS_SQL: &str = r#"
WITH candidates AS MATERIALIZED (
    SELECT pubkey FROM accounts WHERE slot <= $3 AND (
        owner = ANY($1) OR pubkey = ANY($2)
        OR (owner = ANY($4) AND token_mint = ANY($5) AND
            owner IN ('\x06ddf6e1d765a193d9cbe146ceeb79ac1cb485ed5f5b37913a8cf5857eff00a9'::bytea,
                      '\x06ddf6e1ee758fde18425dbce46ccddab61afc4d83b90d27febdf928d8a18bfc'::bytea)))
    UNION
    SELECT pubkey FROM snapshot_accounts WHERE slot <= $3 AND (
        owner = ANY($1) OR pubkey = ANY($2)
        OR (owner = ANY($4) AND token_mint = ANY($5) AND
            owner IN ('\x06ddf6e1d765a193d9cbe146ceeb79ac1cb485ed5f5b37913a8cf5857eff00a9'::bytea,
                      '\x06ddf6e1ee758fde18425dbce46ccddab61afc4d83b90d27febdf928d8a18bfc'::bytea)))
), versions AS (
    SELECT pubkey, owner, lamports, slot, executable, rent_epoch, data, write_version
    FROM accounts WHERE slot <= $3 AND pubkey IN (SELECT pubkey FROM candidates)
    UNION ALL
    SELECT pubkey, owner, lamports, slot, executable, rent_epoch, data, write_version
    FROM snapshot_accounts WHERE slot <= $3 AND pubkey IN (SELECT pubkey FROM candidates)
), latest AS (
    SELECT DISTINCT ON (pubkey) * FROM versions ORDER BY pubkey, slot DESC, write_version DESC, lamports DESC
)
SELECT pubkey, owner, lamports, executable, rent_epoch, data FROM latest WHERE lamports > 0 ORDER BY pubkey
"#;

fn account_requested(
    selection: &SnapshotSelection,
    keys: &[PubkeyDef],
    pubkey: &Pubkey,
    owner: &Pubkey,
    data: &[u8],
) -> bool {
    selection.programs.binary_search(owner).is_ok()
        || keys.iter().any(|key| key.0 == *pubkey)
        || cloudbreak_core::modules::token_mint_filter::matches_token_mint_filters(
            &selection.token_mint_filters,
            owner,
            data,
        )
}

type SharedBuild = Result<(Arc<PhoenixAccountsResponse>, GpaMetricsData), Arc<RpcError>>;

struct SnapshotRequest {
    selection: SnapshotSelection,
    build: BoxFuture<'static, SharedBuild>,
    reply: oneshot::Sender<SharedBuild>,
}

/// Coalesce in-flight work per API state and account coverage. Completed results are not cached.
#[derive(Default)]
pub struct SnapshotWorker {
    sender: OnceLock<mpsc::Sender<SnapshotRequest>>,
}

impl SnapshotWorker {
    async fn resolve(
        &self,
        selection: impl Into<SnapshotSelection>,
        build: BoxFuture<'static, SharedBuild>,
    ) -> SharedBuild {
        let sender = self.sender.get_or_init(|| {
            let (sender, receiver) = mpsc::channel(128);
            tokio::spawn(Self::run(receiver));
            sender
        });
        let (reply, receiver) = oneshot::channel();
        sender
            .send(SnapshotRequest {
                selection: selection.into(),
                build,
                reply,
            })
            .await
            .map_err(|_| Arc::new(RpcError::InternalError))?;
        receiver
            .await
            .map_err(|_| Arc::new(RpcError::InternalError))?
    }

    async fn run(mut receiver: mpsc::Receiver<SnapshotRequest>) {
        let mut waiters: BTreeMap<SnapshotSelection, Vec<oneshot::Sender<SharedBuild>>> =
            BTreeMap::new();
        let mut jobs = FuturesUnordered::new();
        let mut accepting = true;
        loop {
            if !accepting && jobs.is_empty() {
                break;
            }
            tokio::select! {
                // Attach already queued requests before delivering a completed build.
                biased;
                request = receiver.recv(), if accepting => {
                    let Some(request) = request else { accepting = false; continue; };
                    if request.reply.is_closed() { continue; }
                    let key = request.selection;
                    let subscribers = waiters.entry(key.clone()).or_default();
                    if subscribers.is_empty() {
                        let job = tokio::spawn(request.build);
                        jobs.push(async move {
                            (key, job.await.unwrap_or_else(|e| {
                                tracing::error!("Snapshot build task failed: {e}");
                                Err(Arc::new(RpcError::InternalError))
                            }))
                        });
                    }
                    subscribers.push(request.reply);
                }
                Some((key, result)) = jobs.next(), if !jobs.is_empty() => {
                    for reply in waiters.remove(&key).unwrap_or_default() {
                        let _ = reply.send(result.clone());
                    }
                }
            }
        }
    }
}

#[tracing::instrument(skip_all)]
pub async fn get_phoenix_accounts(
    state: &CloudbreakRpcState,
    request: GetPhoenixAccountsConfig,
) -> Result<(Arc<PhoenixAccountsResponse>, GpaMetricsData), RpcError> {
    let config = state
        .phoenix_accounts
        .as_ref()
        .filter(|c| c.enabled)
        .ok_or(RpcError::MethodNotFound)?;
    validate_config(config, state)?;
    let include_twap = request.include_twap.unwrap_or(config.include_twap);
    let programs = request_programs(
        config,
        include_twap,
        &request.additional_program_ids,
        &state.indexer_filter,
    )?;
    let mut token_mint_filters =
        request_mint_filters(config, &request.token_mint_filters, &state.indexer_filter)?;
    token_mint_filters.retain(|filter| programs.binary_search(&filter.token_program.0).is_err());
    let selection = SnapshotSelection {
        programs,
        token_mint_filters,
    };
    let build_selection = selection.clone();
    let build_state = state.clone();
    let result = state
        .phoenix_snapshots
        .resolve(
            selection,
            Box::pin(async move {
                build_snapshot(&build_state, build_selection)
                    .await
                    .map(|(response, metrics)| (Arc::new(response), metrics))
                    .map_err(Arc::new)
            }),
        )
        .await;
    let (response, metrics) = result.map_err(|error| match error.as_ref() {
        RpcError::NodeUnhealthy { .. } => state.node_unhealthy(),
        RpcError::InvalidParamsWithMessage(message) => {
            RpcError::InvalidParamsWithMessage(message.clone())
        }
        // The builder has no caller-specific parameters; other failures are internal.
        error => {
            tracing::error!("Shared snapshot build failed: {error}");
            RpcError::InternalError
        }
    })?;
    validate_min_context_slot(response.context.slot, request.min_context_slot)?;
    Ok((response, metrics))
}

fn validate_min_context_slot(slot: u64, minimum: Option<u64>) -> Result<(), RpcError> {
    if minimum.is_some_and(|min| slot < min) {
        return Err(RpcError::MinContextSlotNotReached { context_slot: slot });
    }
    Ok(())
}

async fn build_snapshot(
    state: &CloudbreakRpcState,
    selection: SnapshotSelection,
) -> Result<(PhoenixAccountsResponse, GpaMetricsData), RpcError> {
    let config = state
        .phoenix_accounts
        .as_ref()
        .ok_or(RpcError::InternalError)?;
    let keys = config.vault_accounts.clone();
    let metrics = GpaMetricsData::new("getPhoenixAccounts".into());
    let started = tokio::time::Instant::now();
    let permit = SNAPSHOT_PERMIT
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| RpcError::InternalError)?;

    let read = async {
        let mut transaction = state
            .database
            .get_postgres_connection_pool()
            .begin()
            .await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            .execute(&mut *transaction)
            .await?;
        let checkpoint = sqlx::query(CHECKPOINT_SQL)
            .fetch_optional(&mut *transaction)
            .await?;
        let Some(checkpoint) = checkpoint else {
            return Ok::<_, sqlx::Error>(None);
        };
        let slot: i64 = checkpoint.try_get("slot")?;
        let count: i64 = checkpoint.try_get("transaction_count")?;
        let hash: String = checkpoint.try_get("blockhash")?;
        if slot < 0 || count < 0 || hash.is_empty() {
            return Ok(None);
        }
        let mut snapshot = AccountSnapshot {
            slot: slot as u64,
            slot_index: (count as u64).checked_sub(1),
            transaction_count: count as u64,
            blockhash: hash,
            programs: Vec::new(),
        };
        let mut groups: BTreeMap<[u8; 32], Vec<SnapshotAccount>> = selection
            .programs
            .iter()
            .copied()
            .chain(
                selection
                    .token_mint_filters
                    .iter()
                    .map(|f| f.token_program.0),
            )
            .map(|p| (p.to_bytes(), Vec::new()))
            .collect();
        let mut bytes = 0usize;
        let mut rows = sqlx::query(ACCOUNTS_SQL)
            .bind(
                selection
                    .programs
                    .iter()
                    .map(|p| p.to_bytes().to_vec())
                    .collect::<Vec<_>>(),
            )
            .bind(
                keys.iter()
                    .map(|p| p.0.to_bytes().to_vec())
                    .collect::<Vec<_>>(),
            )
            .bind(slot)
            .bind(
                selection
                    .token_mint_filters
                    .iter()
                    .map(|f| f.token_program.0.to_bytes().to_vec())
                    .collect::<Vec<_>>(),
            )
            .bind(
                selection
                    .token_mint_filters
                    .iter()
                    .map(|f| f.mint.0.to_bytes().to_vec())
                    .collect::<Vec<_>>(),
            )
            .fetch(&mut *transaction);
        while let Some(row) = rows.next().await {
            let row = row?;
            let data: Vec<u8> = row.try_get("data")?;
            let key: Vec<u8> = row.try_get("pubkey")?;
            let owner: Vec<u8> = row.try_get("owner")?;
            let pubkey = key
                .try_into()
                .map_err(|_| sqlx::Error::Protocol("invalid pubkey".into()))?;
            let owner = owner
                .try_into()
                .map_err(|_| sqlx::Error::Protocol("invalid owner".into()))?;
            if !account_requested(
                &selection,
                &keys,
                &Pubkey::new_from_array(pubkey),
                &Pubkey::new_from_array(owner),
                &data,
            ) {
                continue;
            }
            bytes = bytes.saturating_add(data.len()).saturating_add(128);
            if bytes > MAX_SNAPSHOT_BYTES {
                return Err(sqlx::Error::Protocol("snapshot exceeds size limit".into()));
            }
            let rent: rust_decimal::Decimal = row.try_get("rent_epoch")?;
            groups.entry(owner).or_default().push(SnapshotAccount {
                pubkey,
                owner,
                lamports: row.try_get::<i64, _>("lamports")? as u64,
                executable: row.try_get("executable")?,
                rent_epoch: rent
                    .to_u64()
                    .ok_or_else(|| sqlx::Error::Protocol("invalid rent epoch".into()))?,
                data,
            });
        }
        drop(rows);
        snapshot.programs = groups
            .into_iter()
            .map(|(program_id, accounts)| ProgramAccounts {
                program_id,
                accounts,
            })
            .collect();
        transaction.commit().await?;
        Ok(Some(snapshot))
    };
    let snapshot = tokio::time::timeout(state.queries_timeout, read)
        .await
        .map_err(|_| RpcError::InternalError)?
        .map_err(|e| {
            tracing::error!("Atomic account read failed: {e}");
            RpcError::InternalError
        })?
        .ok_or_else(|| state.node_unhealthy())?;
    validate_vaults(&snapshot, &keys)?;
    metrics.set_db_metrics(started.elapsed().as_secs_f64() * 1000.0, 0.0);
    let context = SnapshotContext {
        slot: snapshot.slot,
        slot_index: snapshot.slot_index,
        transaction_count: snapshot.transaction_count,
        blockhash: snapshot.blockhash.clone(),
        position: "endOfSlot",
    };
    let account_count = snapshot.programs.iter().map(|p| p.accounts.len()).sum();
    let encoding_start = tokio::time::Instant::now();
    let (compressed_bytes, data) = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        encode_snapshot(&snapshot).map(|encoded| (encoded.len(), STANDARD.encode(encoded)))
    })
    .await
    .map_err(|_| RpcError::InternalError)?
    .map_err(|e| {
        tracing::error!("Snapshot encoding failed: {e}");
        RpcError::InternalError
    })?;
    let response = PhoenixAccountsResponse {
        context,
        encoding: SnapshotEncoding::Base64WincodeZstd,
        account_count,
        compressed_bytes,
        data,
    };
    metrics.set_encode_metrics(encoding_start.elapsed().as_secs_f64() * 1000.0);
    Ok((response, metrics))
}

fn validate_vaults(snapshot: &AccountSnapshot, required: &[PubkeyDef]) -> Result<(), RpcError> {
    for key in required {
        let account = snapshot
            .programs
            .iter()
            .flat_map(|p| &p.accounts)
            .find(|a| a.pubkey == key.0.to_bytes())
            .ok_or_else(|| {
                RpcError::InvalidParamsWithMessage(format!(
                    "Required vault {} is absent from the published snapshot",
                    key.0
                ))
            })?;
        let owner = Pubkey::new_from_array(account.owner);
        if !super::is_token_program(&owner)
            || spl_token_2022::extension::StateWithExtensions::<spl_token_2022::state::Account>::unpack(&account.data).is_err() {
            return Err(RpcError::InvalidParamsWithMessage(format!("Required vault {} is not a token account", key.0)));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ready_build(slot: u64) -> SharedBuild {
        Ok((
            Arc::new(PhoenixAccountsResponse {
                context: SnapshotContext {
                    slot,
                    slot_index: Some(1),
                    transaction_count: 2,
                    blockhash: format!("hash{slot}"),
                    position: "endOfSlot",
                },
                encoding: SnapshotEncoding::Base64WincodeZstd,
                account_count: 1,
                compressed_bytes: 3,
                data: "shared-payload".into(),
            }),
            GpaMetricsData::new("getPhoenixAccounts".into()),
        ))
    }

    #[tokio::test]
    async fn overlapping_requests_share_build_and_payload_after_leader_cancellation() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let worker = Arc::new(SnapshotWorker::default());
        let builds = Arc::new(AtomicUsize::new(0));
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let leader = {
            let worker = worker.clone();
            let builds = builds.clone();
            tokio::spawn(async move {
                worker
                    .resolve(
                        vec![Pubkey::new_from_array([1; 32])],
                        Box::pin(async move {
                            builds.fetch_add(1, Ordering::SeqCst);
                            let _ = started_tx.send(());
                            release_rx.await.unwrap();
                            ready_build(100)
                        }),
                    )
                    .await
            })
        };
        started_rx.await.unwrap();
        let mut followers = Vec::new();
        for _ in 0..10 {
            let (reply, receiver) = oneshot::channel();
            let builds = builds.clone();
            worker
                .sender
                .get()
                .unwrap()
                .send(SnapshotRequest {
                    selection: vec![Pubkey::new_from_array([1; 32])].into(),
                    reply,
                    build: Box::pin(async move {
                        builds.fetch_add(1, Ordering::SeqCst);
                        ready_build(999)
                    }),
                })
                .await
                .unwrap();
            followers.push(receiver);
        }
        leader.abort(); // cancellation must not cancel the shared build
        assert!(leader.await.err().unwrap().is_cancelled());
        release_tx.send(()).unwrap();
        let first = followers.remove(0).await.unwrap().unwrap().0;
        assert_eq!(first.context.slot, 100);
        assert!(validate_min_context_slot(first.context.slot, Some(100)).is_ok());
        assert!(matches!(
            validate_min_context_slot(first.context.slot, Some(101)),
            Err(RpcError::MinContextSlotNotReached { context_slot: 100 })
        ));
        for receiver in followers {
            let response = receiver.await.unwrap().unwrap().0;
            assert!(Arc::ptr_eq(&first, &response));
        }
        assert_eq!(builds.load(Ordering::SeqCst), 1);
        // Completed results are not a stale cache: the next request builds anew.
        let builds_for_next = builds.clone();
        let next = worker
            .resolve(
                vec![Pubkey::new_from_array([1; 32])],
                Box::pin(async move {
                    builds_for_next.fetch_add(1, Ordering::SeqCst);
                    ready_build(101)
                }),
            )
            .await
            .unwrap()
            .0;
        assert_eq!(next.context.slot, 101);
        assert_eq!(builds.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn different_program_sets_are_not_shared_and_failed_jobs_can_retry() {
        let worker = Arc::new(SnapshotWorker::default());
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let without_twap = {
            let worker = worker.clone();
            tokio::spawn(async move {
                worker
                    .resolve(
                        vec![Pubkey::new_from_array([1; 32])],
                        Box::pin(async move {
                            started_tx.send(()).unwrap();
                            release_rx.await.unwrap();
                            ready_build(100)
                        }),
                    )
                    .await
            })
        };
        started_rx.await.unwrap();
        let with_twap = worker
            .resolve(
                vec![
                    Pubkey::new_from_array([1; 32]),
                    Pubkey::new_from_array([2; 32]),
                ],
                Box::pin(async { ready_build(200) }),
            )
            .await
            .unwrap()
            .0;
        assert_eq!(with_twap.context.slot, 200);
        assert!(!without_twap.is_finished());
        release_tx.send(()).unwrap();
        assert_eq!(without_twap.await.unwrap().unwrap().0.context.slot, 100);
        let failure = worker
            .resolve(
                vec![Pubkey::new_from_array([1; 32])],
                Box::pin(async { panic!("injected build failure") }),
            )
            .await;
        assert!(matches!(
            failure.err().unwrap().as_ref(),
            RpcError::InternalError
        ));
        assert_eq!(
            worker
                .resolve(
                    vec![Pubkey::new_from_array([1; 32])],
                    Box::pin(async { ready_build(201) })
                )
                .await
                .unwrap()
                .0
                .context
                .slot,
            201
        );
    }

    #[test]
    fn caller_programs_are_normalized_and_require_full_index_coverage() {
        use cloudbreak_core::AccountSelectorConfig;
        let key = |n| PubkeyDef(Pubkey::new_from_array([n; 32]));
        let config = PhoenixAccountsConfig {
            program_ids: vec![key(1)],
            twap_program_ids: vec![key(4)],
            ..Default::default()
        };
        let filters = AccountSelectorConfig {
            include: vec![key(1), key(2), key(3), key(4)],
            ..Default::default()
        };
        let first = request_programs(&config, false, &[key(3), key(2), key(3)], &filters).unwrap();
        let second = request_programs(&config, false, &[key(2), key(3)], &filters).unwrap();
        assert_eq!(first, vec![key(1).0, key(2).0, key(3).0]);
        assert_eq!(first, second); // same worker sharing key despite order/duplicates
        assert!(matches!(
            request_programs(&config, false, &[key(5)], &filters),
            Err(RpcError::KeyExcludedFromSecondaryIndex { .. })
        ));
        // An exact-account inclusion is not coverage of that account's entire program.
        let partial = AccountSelectorConfig {
            accounts: vec![key(5)],
            ..filters.clone()
        };
        assert!(request_programs(&config, false, &[key(5)], &partial).is_err());
        assert_eq!(
            request_programs(&config, true, &[], &filters).unwrap(),
            vec![key(1).0, key(4).0]
        );
        // An explicit Flicker program ID works without the TWAP convenience option.
        assert_eq!(
            request_programs(&config, false, &[key(4)], &filters).unwrap(),
            vec![key(1).0, key(4).0]
        );
        assert!(request_programs(&config, false, &vec![key(2); 65], &filters).is_err());
        assert!(
            serde_json::from_str::<GetPhoenixAccountsConfig>(
                r#"{"additionalProgramIds":["invalid-key"]}"#
            )
            .is_err()
        );
        let json = format!(
            r#"{{"additionalProgramIds":["{}","{}"]}}"#,
            key(2).0,
            key(3).0
        );
        assert_eq!(
            serde_json::from_str::<GetPhoenixAccountsConfig>(&json)
                .unwrap()
                .additional_program_ids
                .len(),
            2
        );
    }

    #[test]
    fn selection_deduplicates_programs_and_controls_twap() {
        let key = |n| PubkeyDef(Pubkey::new_from_array([n; 32]));
        let config = PhoenixAccountsConfig {
            program_ids: vec![key(2), key(1)],
            additional_program_ids: vec![key(2), key(3)],
            twap_program_ids: vec![key(4)],
            ..Default::default()
        };
        assert_eq!(
            selected_programs(&config, false),
            vec![key(1).0, key(2).0, key(3).0]
        );
        assert_eq!(selected_programs(&config, true).len(), 4);
        assert!(
            serde_json::from_str::<GetPhoenixAccountsConfig>(r#"{"encoding":"base64"}"#).is_err()
        );
        assert!(serde_json::from_str::<GetPhoenixAccountsConfig>(r#"{"slotIndex":1}"#).is_err());
        assert!(
            serde_json::from_str::<GetPhoenixAccountsConfig>(r#"{"commitment":"processed"}"#)
                .is_err()
        );
    }

    #[test]
    fn caller_mint_filters_require_exact_coverage_and_normalize_coalescing_keys() {
        use cloudbreak_core::modules::token_mint_filter::{
            TOKEN_2022_PROGRAM_ID, TOKEN_PROGRAM_ID,
        };
        let first = TokenMintFilter {
            mint: PubkeyDef(Pubkey::new_unique()),
            token_program: PubkeyDef(TOKEN_PROGRAM_ID),
        };
        let second = TokenMintFilter {
            mint: PubkeyDef(Pubkey::new_unique()),
            ..first.clone()
        };
        let config = PhoenixAccountsConfig {
            token_mint_filters: vec![first.clone()],
            ..Default::default()
        };
        let indexer = AccountSelectorConfig {
            include: vec![PubkeyDef(Pubkey::new_unique())],
            token_mint_filters: vec![first.clone(), second.clone()],
            ..Default::default()
        };
        let a = request_mint_filters(
            &config,
            &[second.clone(), first.clone(), second.clone()],
            &indexer,
        )
        .unwrap();
        let b = request_mint_filters(&config, std::slice::from_ref(&second), &indexer).unwrap();
        assert_eq!(a, b);
        let wrong_program = TokenMintFilter {
            token_program: PubkeyDef(TOKEN_2022_PROGRAM_ID),
            ..first.clone()
        };
        assert!(matches!(
            request_mint_filters(&config, &[wrong_program], &indexer),
            Err(RpcError::KeyExcludedFromSecondaryIndex { .. })
        ));
        assert!(request_mint_filters(&config, &vec![first.clone(); 65], &indexer).is_err());
        let config = PhoenixAccountsConfig::default();
        let exact_only = AccountSelectorConfig {
            accounts: vec![first.mint.clone()],
            token_mint_filters: vec![],
            ..indexer.clone()
        };
        assert!(!exact_only.covers_token_mint(&first));
        assert!(
            request_mint_filters(
                &config,
                &[TokenMintFilter {
                    token_program: PubkeyDef(Pubkey::new_unique()),
                    ..first.clone()
                }],
                &indexer
            )
            .is_err()
        );
        let parsed: GetPhoenixAccountsConfig = serde_json::from_value(
            serde_json::json!({"tokenMintFilters":[{"mint": first.mint.0.to_string()}]}),
        )
        .unwrap();
        assert_eq!(parsed.token_mint_filters, vec![first.clone()]);
        let programs = vec![Pubkey::new_unique()];
        assert_ne!(
            SnapshotSelection {
                programs: programs.clone(),
                token_mint_filters: vec![first]
            },
            SnapshotSelection {
                programs,
                token_mint_filters: vec![second]
            }
        );
    }

    #[test]
    fn missing_or_invalid_vaults_fail_closed() {
        let key = PubkeyDef(Pubkey::new_from_array([1; 32]));
        let mut snapshot = AccountSnapshot {
            slot: 10,
            slot_index: None,
            transaction_count: 0,
            blockhash: "hash".into(),
            programs: vec![],
        };
        assert!(validate_vaults(&snapshot, std::slice::from_ref(&key)).is_err());
        let mut data = vec![0; 165];
        data[108] = 1; // initialized SPL Token account
        snapshot.programs.push(ProgramAccounts {
            program_id: super::super::LEGACY_TOKEN_PROGRAM_ID.to_bytes(),
            accounts: vec![SnapshotAccount {
                pubkey: key.0.to_bytes(),
                owner: super::super::LEGACY_TOKEN_PROGRAM_ID.to_bytes(),
                data,
                lamports: 1,
                executable: false,
                rent_epoch: 0,
            }],
        });
        assert!(validate_vaults(&snapshot, std::slice::from_ref(&key)).is_ok());
        snapshot.programs[0].accounts[0].data = vec![0; 82]; // mint, not vault
        assert!(validate_vaults(&snapshot, &[key]).is_err());
    }

    #[tokio::test]
    #[ignore = "requires a disposable local PostgreSQL database"]
    async fn token_mint_snapshot_excludes_stale_closed_and_reinitialized_accounts() {
        use crate::modules::{
            cache::GpaProcessor, supply_cache::SupplySnapshot, vote_accounts_cache::StakesSnapshot,
        };
        use cloudbreak_core::modules::{
            account_snapshot::decode_snapshot,
            processed::ProcessedAccounts,
            token_mint_filter::{TOKEN_2022_PROGRAM_ID, TOKEN_PROGRAM_ID},
        };
        use cloudbreak_core::{
            MethodSection, ProcessedCommitmentBehavior, UnhealthyResponseBehavior,
        };
        use std::{sync::RwLock, time::Duration};

        let url = std::env::var("CLOUDBREAK_TEST_DATABASE_URL").unwrap();
        let bootstrap = sqlx::PgPool::connect(&url).await.unwrap();
        let schema = format!("mint_snapshot_{}", std::process::id());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&bootstrap)
            .await
            .unwrap();
        let mut options = sea_orm::ConnectOptions::new(url);
        options
            .max_connections(2)
            .set_schema_search_path(schema.clone());
        let db = sea_orm::Database::connect(options).await.unwrap();
        let pool = db.get_postgres_connection_pool().clone();
        sqlx::raw_sql("CREATE TABLE slots (slot bigint, commitment int, health bool); CREATE TABLE service_health (id int, healthy bool); CREATE TABLE atomic_account_checkpoint (id int, slot bigint, transaction_count bigint, blockhash text); CREATE TABLE accounts (pubkey bytea, owner bytea, lamports bigint, slot bigint, executable bool, rent_epoch numeric, data bytea, write_version bigint DEFAULT 0, token_mint bytea GENERATED ALWAYS AS (SUBSTRING(data FROM 1 FOR 32)) STORED); CREATE TABLE snapshot_accounts (LIKE accounts INCLUDING GENERATED); INSERT INTO slots VALUES (100,1,true); INSERT INTO service_health VALUES (1,true); INSERT INTO atomic_account_checkpoint VALUES (1,100,3,'hash100');").execute(&pool).await.unwrap();
        let key = |n| Pubkey::new_from_array([n; 32]);
        let mint_a = key(101);
        let mint_b = key(102);
        let phoenix = key(100);
        let filters = vec![
            TokenMintFilter {
                mint: PubkeyDef(mint_a),
                token_program: PubkeyDef(TOKEN_PROGRAM_ID),
            },
            TokenMintFilter {
                mint: PubkeyDef(mint_b),
                token_program: PubkeyDef(TOKEN_2022_PROGRAM_ID),
            },
        ];
        for (id, owner, mint, slot, lamports) in [
            (1, phoenix, mint_a, 90, 1),
            (2, TOKEN_PROGRAM_ID, mint_b, 90, 1), // required vault, outside mint filters
            (3, TOKEN_PROGRAM_ID, mint_a, 90, 1),
            (3, TOKEN_PROGRAM_ID, mint_b, 100, 2), // reinitialized, same token owner
            (4, TOKEN_PROGRAM_ID, mint_a, 90, 1),
            (4, Pubkey::default(), mint_a, 100, 0), // closed, system owner
            (5, TOKEN_PROGRAM_ID, mint_a, 90, 1),
            (5, Pubkey::default(), mint_a, 100, 2), // left token program
            (6, TOKEN_PROGRAM_ID, mint_a, 90, 1),
            (6, TOKEN_PROGRAM_ID, mint_a, 101, 2), // future write must be excluded
            (7, TOKEN_PROGRAM_ID, mint_b, 100, 1), // wrong program/mint pair
            (8, TOKEN_2022_PROGRAM_ID, mint_b, 100, 1),
            (9, TOKEN_2022_PROGRAM_ID, mint_a, 100, 1), // wrong program/mint pair
            (10, TOKEN_PROGRAM_ID, mint_a, 100, 1),
        ] {
            let mut data = vec![0; 165];
            data[..32].copy_from_slice(mint.as_ref());
            data[108] = 1;
            let table = if slot == 90 {
                "snapshot_accounts"
            } else {
                "accounts"
            };
            sqlx::query(&format!("INSERT INTO {table} (pubkey,owner,lamports,slot,executable,rent_epoch,data) VALUES ($1,$2,$3,$4,false,0,$5)"))
                .bind(key(id).to_bytes().to_vec()).bind(owner.to_bytes().to_vec())
                .bind(lamports as i64).bind(slot as i64).bind(data).execute(&pool).await.unwrap();
        }
        let mut state = CloudbreakRpcState::new(
            db,
            Duration::from_secs(10),
            None,
            None,
            Arc::new(AccountSelectorConfig {
                include: vec![PubkeyDef(phoenix)],
                accounts: vec![PubkeyDef(key(2))],
                token_mint_filters: filters.clone(),
                ..Default::default()
            }),
            1,
            None,
            Duration::from_secs(10),
            ProcessedCommitmentBehavior::default(),
            UnhealthyResponseBehavior::default(),
            GpaProcessor::new(None),
            "genesis".into(),
            false,
            Arc::new(RwLock::new(Arc::new(StakesSnapshot::empty()))),
            100,
            false,
            false,
            Arc::new(RwLock::new(Arc::new(SupplySnapshot::default()))),
            MethodSection::default(),
            MethodSection::default(),
            ProcessedAccounts::default(),
        );
        state.phoenix_accounts = Some(PhoenixAccountsConfig {
            enabled: true,
            program_ids: vec![PubkeyDef(phoenix)],
            vault_accounts: vec![PubkeyDef(key(2))],
            ..Default::default()
        });
        let request = || GetPhoenixAccountsConfig {
            token_mint_filters: filters.clone(),
            ..Default::default()
        };
        let (one, two) = tokio::join!(
            get_phoenix_accounts(&state, request()),
            get_phoenix_accounts(&state, request())
        );
        let response = one.unwrap().0;
        assert!(Arc::ptr_eq(&response, &two.unwrap().0));
        assert_eq!(response.context.slot, 100);
        assert_eq!(response.context.slot_index, Some(2));
        let snapshot = decode_snapshot(&STANDARD.decode(&response.data).unwrap()).unwrap();
        let mut accounts: Vec<_> = snapshot.programs.iter().flat_map(|p| &p.accounts).collect();
        accounts.sort_by_key(|account| account.pubkey);
        assert_eq!(
            accounts.iter().map(|a| a.pubkey).collect::<Vec<_>>(),
            [1, 2, 6, 8, 10].map(|n| key(n).to_bytes())
        );
        assert_eq!(
            accounts
                .iter()
                .find(|a| a.pubkey == key(6).to_bytes())
                .unwrap()
                .lamports,
            1
        );
        assert!(
            snapshot
                .programs
                .iter()
                .all(|p| p.accounts.iter().all(|a| a.owner == p.program_id))
        );
        assert_eq!(response.account_count, 5);
        assert!(matches!(
            get_phoenix_accounts(
                &state,
                GetPhoenixAccountsConfig {
                    additional_program_ids: vec![PubkeyDef(TOKEN_PROGRAM_ID)],
                    ..Default::default()
                }
            )
            .await,
            Err(RpcError::KeyExcludedFromSecondaryIndex { .. })
        ));
        // No mint selections: only base Phoenix accounts and mandatory vaults.
        assert_eq!(
            get_phoenix_accounts(&state, GetPhoenixAccountsConfig::default())
                .await
                .unwrap()
                .0
                .account_count,
            2
        );
        // The balance query must retain mint coverage without fetching a large full account payload.
        let balance_sql = include_str!("../db/getBalance.sql")
            .replace(
                "$1",
                &format!("'\\x{}'::bytea", hex::encode(key(6).to_bytes())),
            )
            .replace("$2", "100");
        let row = sqlx::raw_sql(&balance_sql).fetch_one(&pool).await.unwrap();
        assert!(state.indexer_filter.is_account_selected(
            &key(6),
            &TOKEN_PROGRAM_ID,
            &row.get::<Vec<u8>, _>("data")
        ));
        drop(state);
        pool.close().await;
        sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
            .execute(&bootstrap)
            .await
            .unwrap();
    }

    /// Run against a disposable database: CLOUDBREAK_TEST_DATABASE_URL=... cargo test ... --ignored
    #[tokio::test]
    #[ignore = "requires a disposable local PostgreSQL database"]
    async fn repeatable_read_keeps_checkpoint_accounts_and_closures_atomic() {
        let url = std::env::var("CLOUDBREAK_TEST_DATABASE_URL").unwrap();
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await
            .unwrap();
        // A per-test schema prevents touching existing Cloudbreak data.
        let schema = format!("atomic_test_{}", std::process::id());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&pool)
            .await
            .unwrap();
        let mut reader = pool.acquire().await.unwrap();
        let mut writer = pool.acquire().await.unwrap();
        for connection in [&mut reader, &mut writer] {
            sqlx::query(&format!("SET search_path TO {schema}"))
                .execute(&mut **connection)
                .await
                .unwrap();
        }
        sqlx::raw_sql("CREATE TABLE slots (slot bigint, commitment int, health bool); CREATE TABLE service_health (id int, healthy bool); CREATE TABLE atomic_account_checkpoint (id int, slot bigint, transaction_count bigint, blockhash text); CREATE TABLE accounts (pubkey bytea, owner bytea, lamports bigint, slot bigint, executable bool, rent_epoch numeric, data bytea, write_version bigint DEFAULT 0, token_mint bytea GENERATED ALWAYS AS (SUBSTRING(data FROM 1 FOR 32)) STORED); CREATE TABLE snapshot_accounts (LIKE accounts); INSERT INTO slots VALUES (100,1,true); INSERT INTO service_health VALUES (1,true); INSERT INTO atomic_account_checkpoint VALUES (1,100,3,'hash100');").execute(&mut *writer).await.unwrap();
        let owner = vec![9u8; 32];
        for (key, slot, lamports) in [
            (1u8, 90i64, 1i64),
            (1, 100, 2),
            (2, 90, 1),
            (2, 100, 0),
            (3, 101, 9),
        ] {
            sqlx::query("INSERT INTO accounts (pubkey,owner,lamports,slot,executable,rent_epoch,data,write_version) VALUES ($1,$2,$3,$4,false,0,$5,0)")
                .bind(vec![key; 32])
                .bind(&owner)
                .bind(lamports)
                .bind(slot)
                .bind(vec![key])
                .execute(&mut *writer)
                .await
                .unwrap();
        }
        // A vault from the snapshot table has a different owner and must be selected by key.
        sqlx::query("INSERT INTO snapshot_accounts (pubkey,owner,lamports,slot,executable,rent_epoch,data,write_version) VALUES ($1,$2,5,90,false,0,$3,0)")
            .bind(vec![4u8; 32])
            .bind(vec![7u8; 32])
            .bind(vec![4u8])
            .execute(&mut *writer)
            .await
            .unwrap();
        // A caller-selected extra program must use the same boundary as the core program.
        sqlx::query("INSERT INTO snapshot_accounts VALUES ($1,$2,6,99,false,0,$3,0)")
            .bind(vec![5u8; 32])
            .bind(vec![8u8; 32])
            .bind(vec![5u8])
            .execute(&mut *writer)
            .await
            .unwrap();
        sqlx::query("INSERT INTO accounts VALUES ($1,$2,9,101,false,0,$3,0)")
            .bind(vec![5u8; 32])
            .bind(vec![8u8; 32])
            .bind(vec![6u8])
            .execute(&mut *writer)
            .await
            .unwrap();
        sqlx::raw_sql("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *reader)
            .await
            .unwrap();
        let checkpoint = sqlx::query(CHECKPOINT_SQL)
            .fetch_one(&mut *reader)
            .await
            .unwrap();
        assert_eq!(checkpoint.get::<i64, _>("slot"), 100);
        // Next slot publishes and cleanup runs while the first read remains open.
        sqlx::raw_sql("UPDATE slots SET slot=101; UPDATE atomic_account_checkpoint SET slot=101,transaction_count=4,blockhash='hash101'; DELETE FROM accounts WHERE slot<100;").execute(&mut *writer).await.unwrap();
        let rows = sqlx::query(ACCOUNTS_SQL)
            .bind(vec![owner.clone(), vec![8u8; 32]])
            .bind(vec![vec![4u8; 32]])
            .bind(100i64)
            .bind(Vec::<Vec<u8>>::new())
            .bind(Vec::<Vec<u8>>::new())
            .fetch_all(&mut *reader)
            .await
            .unwrap();
        assert_eq!(rows.len(), 3); // core + vault + additional program; closures/future rows excluded
        assert_eq!(rows[0].get::<i64, _>("lamports"), 2);
        assert_eq!(rows[1].get::<Vec<u8>, _>("owner"), vec![7u8; 32]);
        assert_eq!(rows[2].get::<Vec<u8>, _>("owner"), vec![8u8; 32]);
        assert_eq!(rows[2].get::<i64, _>("lamports"), 6);
        assert_eq!(
            sqlx::query(CHECKPOINT_SQL)
                .fetch_one(&mut *reader)
                .await
                .unwrap()
                .get::<i64, _>("slot"),
            100
        );
        sqlx::raw_sql("COMMIT; INSERT INTO slots VALUES (102,2,true);")
            .execute(&mut *reader)
            .await
            .unwrap();
        assert!(
            sqlx::query(CHECKPOINT_SQL)
                .fetch_optional(&mut *reader)
                .await
                .unwrap()
                .is_none()
        );
        sqlx::raw_sql(
            "DELETE FROM slots WHERE commitment=2; UPDATE service_health SET healthy=false;",
        )
        .execute(&mut *reader)
        .await
        .unwrap();
        assert!(
            sqlx::query(CHECKPOINT_SQL)
                .fetch_optional(&mut *reader)
                .await
                .unwrap()
                .is_none()
        );
        sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
            .execute(&mut *writer)
            .await
            .unwrap();
    }
}
