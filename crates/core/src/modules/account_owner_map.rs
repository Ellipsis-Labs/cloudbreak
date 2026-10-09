// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Optional module that maintains an in-memory cache mapping accounts to their
//! owners and their latest observed slot. Slot tracking prevents older gap repairs
//! from removing a mapping still needed to mask a later closure or mint change.
//!
//! Main use cases:
//! - Simplifies the closed-accounts insertion SQL by already knowing each
//!   account's owner at the time of closure.
//! - Enables account-owner-change detection for use cases where tracking
//!   ownership transitions is relevant.
//!
//! TODO: This cache could potentially be used to speed up snapshot
//! deduplication at startup.

use sea_orm::{ConnectionTrait, DatabaseConnection, ExecResult, Statement, Value};
use solana_pubkey::Pubkey;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock, RwLock},
    time::Duration,
};
use tokio::time::timeout;

pub static ACCOUNTS_OWNER_MAP: OnceLock<Arc<RwLock<HashMap<Pubkey, AccountOwnerItem>>>> =
    OnceLock::new();

/// Map of accounts to their owners and latest observed slot
///
/// Handles internally optional behavior, so it can be used even if the module is disabled
/// and it will be a no-op.
#[derive(Clone, Default)]
pub struct AccountOwnerMap {
    /// If set to `None`, it will efectivelly make the module a no-op
    accounts: Option<Arc<RwLock<HashMap<Pubkey, AccountOwnerItem>>>>,
    db: DatabaseConnection,
    query_timeout: Duration,
    /// List of (pubkey, owner) pairs that have changed their owner for a given slot
    /// This saves the old owner being overwritten so can later insert a mock "closed account" mask
    /// for the old owner.
    changed_owners: Arc<Mutex<HashMap<u64, Vec<ChangedOwner>>>>,
}

#[derive(Clone, Debug)]
pub struct ChangedOwner {
    /// The pubkey of the account that has changed its owner
    pub pubkey: Pubkey,
    /// The old owner
    pub owner: Pubkey,
}

#[derive(Clone, Debug)]
pub struct AccountOwnerItem {
    pub owner: Pubkey,
    pub slot: u64,
}

impl AccountOwnerMap {
    pub fn new(db: DatabaseConnection, query_timeout: Duration) -> Self {
        let accounts = Arc::new(RwLock::new(HashMap::new()));

        ACCOUNTS_OWNER_MAP
            .set(accounts.clone())
            .expect("ACCOUNTS_OWNER_MAP already set");

        Self {
            accounts: Some(accounts),
            db,
            query_timeout,
            changed_owners: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.accounts.is_some()
    }

    /// Inserts the account or updates the owner if the account already exists
    ///
    /// Note: if module is disabled, this will be a no-op
    ///
    /// Older repairs cannot overwrite a newer owner or last-seen slot.
    pub fn upsert_account(&self, pubkey: &Vec<u8>, owner: &Vec<u8>, slot: u64) {
        if let Some(accounts) = &self.accounts {
            let pubkey = Pubkey::try_from(pubkey.as_slice()).unwrap();
            let owner = Pubkey::try_from(owner.as_slice()).unwrap();

            // Slot/owner checks and insertion share one lock, including concurrent snapshot workers.
            let mut map = accounts.write().expect("Failed to write accounts");
            if let Some(item) = map.get(&pubkey) {
                if item.slot >= slot {
                    return;
                }
                if item.owner != owner {
                    self.changed_owners
                        .lock()
                        .expect("Failed to lock changed_owners")
                        .entry(slot)
                        .or_default()
                        .push(ChangedOwner {
                            pubkey,
                            owner: item.owner,
                        });
                }
            }
            map.insert(pubkey, AccountOwnerItem { owner, slot });
        }
    }

    /// Returns the (pubkeys, owners) pairs that owner-route the closed-account cleanup at
    /// finalization: closed accounts present in the map plus the slot's changed-owner pairs.
    ///
    /// Read-only: [`Self::save_closed_accounts`] later drains the same entries for the mask insert.
    pub fn closed_cleanup_pairs(
        &self,
        closed_accounts: &[Vec<u8>],
        slot: u64,
    ) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
        let mut pubkeys = Vec::new();
        let mut owners = Vec::new();
        if let Some(accounts) = &self.accounts {
            let map = accounts.read().expect("Failed to read accounts");
            for pubkey_bytes in closed_accounts {
                let pubkey = Pubkey::try_from(pubkey_bytes.as_slice()).unwrap();
                if let Some(item) = map.get(&pubkey) {
                    pubkeys.push(pubkey_bytes.clone());
                    owners.push(item.owner.to_bytes().to_vec());
                }
            }
        }
        if let Some(changed) = self
            .changed_owners
            .lock()
            .expect("Failed to lock changed_owners")
            .get(&slot)
        {
            for ChangedOwner { pubkey, owner } in changed {
                pubkeys.push(pubkey.to_bytes().to_vec());
                owners.push(owner.to_bytes().to_vec());
            }
        }
        (pubkeys, owners)
    }

    /// For accounts present in the map, saves the mock "closed account" mask into the DB using
    /// the previous owner and the new slot.
    ///
    /// It will only insert closed accounts that are present in the map.
    /// It will remove the accounts from the map.
    pub async fn save_closed_accounts(
        &self,
        closed_accounts: Vec<Vec<u8>>,
        slot: u64,
    ) -> Result<ExecResult, sea_orm::DbErr> {
        let accounts = self.accounts.as_ref().expect("AccountOwnerMap not enabled");

        let (mut pubkeys, mut owners) = {
            let mut map = accounts.write().expect("Failed to write accounts");
            let mut pubkeys = Vec::with_capacity(closed_accounts.len());
            let mut owners = Vec::with_capacity(closed_accounts.len());

            for pubkey_bytes in &closed_accounts {
                let pubkey = Pubkey::try_from(pubkey_bytes.as_slice()).unwrap();

                // A repair below a newer live version must retain its closure-tracking entry.
                if map.get(&pubkey).is_some_and(|item| item.slot > slot) {
                    continue;
                }
                if let Some(item) = map.remove(&pubkey) {
                    pubkeys.push(pubkey_bytes.clone());
                    owners.push(item.owner.to_bytes().to_vec());
                }
            }

            (pubkeys, owners)
        };

        // If there is accounts that have changed their owner for this slot, we need to insert a mock
        //  "closed account" mask for the old owner
        let changed_owners_for_slot = {
            let mut guard = self
                .changed_owners
                .lock()
                .expect("Failed to lock changed_owners");

            guard.remove(&slot)
        };

        if let Some(changed_owners_for_slot) = changed_owners_for_slot {
            for ChangedOwner { pubkey, owner } in changed_owners_for_slot {
                pubkeys.push(pubkey.to_bytes().to_vec());
                owners.push(owner.to_bytes().to_vec());
            }
        }

        let db = self.db.clone();
        let insert_closed_account_sql = include_str!("../db/insertClosedAccountWithMap.sql");
        let query_timeout = self.query_timeout;

        let query = db.execute(Statement::from_sql_and_values(
            sea_orm::DatabaseBackend::Postgres,
            insert_closed_account_sql,
            vec![
                Value::Array(
                    sea_orm::sea_query::ArrayType::Bytes,
                    Some(Box::new(
                        pubkeys
                            .into_iter()
                            .map(|pubkey| Value::Bytes(Some(Box::new(pubkey))))
                            .collect(),
                    )),
                ),
                Value::Array(
                    sea_orm::sea_query::ArrayType::Bytes,
                    Some(Box::new(
                        owners
                            .into_iter()
                            .map(|owner| Value::Bytes(Some(Box::new(owner))))
                            .collect(),
                    )),
                ),
                Value::BigInt(Some(slot as i64)),
            ],
        ));

        timeout(query_timeout, query)
                .await
                .unwrap_or_else(|elapsed| {
                    tracing::error!(target: "save_closed_accounts_with_map", "insert_closed_accounts with map timeout ERROR: {}", elapsed);
                    Err(sea_orm::DbErr::RecordNotInserted)
                })
    }

    pub fn get_owner(&self, pubkey: &Pubkey) -> Option<Pubkey> {
        let accounts = self.accounts.as_ref()?;
        let guard = accounts.read().expect("Failed to read accounts");
        guard.get(pubkey).map(|item| item.owner)
    }

    /// Checks if (in case this was a tracked account) the owner has changed since the last time
    ///  it was seen
    pub fn check_updated_account_owner(&self, pubkey: Pubkey, owner: Pubkey, slot: u64) -> bool {
        let mut owner_changed = false;

        if let Some(accounts) = &self.accounts {
            let guard = accounts.read().expect("Failed to read accounts");
            if let Some(item) = guard.get(&pubkey)
                && (item.owner != owner && item.slot < slot)
            {
                owner_changed = true;
            }
        }

        owner_changed
    }

    /// Mask a previously indexed account that leaves the selection, including same-owner mint changes.
    /// `save_closed_accounts` removes it from the map after writing the old-owner mask.
    pub fn account_to_be_deleted(
        &self,
        pubkey: &Vec<u8>,
        _owner: &Vec<u8>,
        slot: u64,
        is_new_owner_included: bool,
    ) -> bool {
        let pubkey = Pubkey::try_from(pubkey.as_slice()).unwrap();
        if is_new_owner_included {
            return false;
        }
        let Some(accounts) = &self.accounts else {
            return false;
        };
        // A same-owner token account can be reinitialized for an unselected mint.
        accounts
            .read()
            .expect("Failed to read accounts")
            .get(&pubkey)
            .is_some_and(|item| item.slot < slot)
    }

    pub fn get_map_size(
        accounts: &Arc<RwLock<HashMap<Pubkey, AccountOwnerItem>>>,
    ) -> (usize, usize) {
        let capacity = accounts.read().expect("Failed to read accounts").capacity();
        let items = accounts.read().expect("Failed to read accounts").len();

        // Actual bucket count: next power of 2 >= capacity * 8 / 7
        let raw = (capacity * 8).div_ceil(7);
        let buckets = raw.next_power_of_two();
        let per_bucket = size_of::<(Pubkey, AccountOwnerItem)>(); // key-value pair
        let control_bytes = buckets + 16; // 1 byte per bucket + SIMD padding
        let bytes = buckets * per_bucket + control_bytes;

        (bytes, items)
    }

    // It will return the body string for the debug endpoint
    pub fn debug_accounts_owner_map() -> String {
        let accounts = ACCOUNTS_OWNER_MAP.get();

        match accounts {
            Some(accounts) => {
                let (bytes, items) = AccountOwnerMap::get_map_size(accounts);

                let mb = bytes as f64 / 1024.0 / 1024.0;
                format!("AccountOwnerMap: {:.2} MB, {} items", mb, items)
            }
            None => "AccountOwnerMap: not initialized".to_string(),
        }
    }
}

#[cfg(test)]
mod mint_selection_tests {
    use super::*;

    #[test]
    fn token_mint_reinitialization_masks_same_owner_without_masking_older_repairs() {
        let key = Pubkey::new_unique();
        let owner = Pubkey::new_unique();
        let map = AccountOwnerMap {
            accounts: Some(Arc::new(RwLock::new(HashMap::from([(
                key,
                AccountOwnerItem { owner, slot: 100 },
            )])))),
            ..Default::default()
        };
        let key_bytes = key.to_bytes().to_vec();
        let owner_bytes = owner.to_bytes().to_vec();
        assert!(map.account_to_be_deleted(&key_bytes, &owner_bytes, 101, false));
        map.upsert_account(&key_bytes, &owner_bytes, 105);
        assert!(!map.account_to_be_deleted(&key_bytes, &owner_bytes, 103, false));
        assert!(map.account_to_be_deleted(&key_bytes, &owner_bytes, 106, false));
        assert!(!map.account_to_be_deleted(&key_bytes, &owner_bytes, 106, true));
        assert!(!map.account_to_be_deleted(&key_bytes, &owner_bytes, 99, false));
        assert!(!map.account_to_be_deleted(
            &Pubkey::new_unique().to_bytes().to_vec(),
            &owner_bytes,
            101,
            false
        ));
        assert!(!AccountOwnerMap::default().account_to_be_deleted(
            &key_bytes,
            &owner_bytes,
            101,
            false
        ));
    }
    #[tokio::test]
    #[ignore = "requires disposable PostgreSQL via CLOUDBREAK_TEST_DATABASE_URL"]
    async fn mint_reinitialization_masks_and_older_repair_preserves_future_closure_tracking() {
        let url = std::env::var("CLOUDBREAK_TEST_DATABASE_URL").unwrap();
        let bootstrap = sea_orm::Database::connect(&url).await.unwrap();
        let schema = format!("mint_masks_{}", std::process::id());
        bootstrap
            .execute_unprepared(&format!("CREATE SCHEMA {schema}"))
            .await
            .unwrap();
        let mut options = sea_orm::ConnectOptions::new(url);
        options
            .max_connections(1)
            .set_schema_search_path(schema.clone());
        let db = sea_orm::Database::connect(options).await.unwrap();
        db.execute_unprepared("CREATE TABLE accounts (pubkey bytea, owner bytea, lamports bigint, slot bigint, executable bool, rent_epoch numeric, data bytea, write_version bigint);").await.unwrap();
        let key = Pubkey::new_unique();
        let owner = crate::modules::token_mint_filter::TOKEN_PROGRAM_ID;
        let key_bytes = key.to_bytes().to_vec();
        let owner_bytes = owner.to_bytes().to_vec();
        for slot in [100i64, 105] {
            db.execute(Statement::from_sql_and_values(
                sea_orm::DatabaseBackend::Postgres,
                "INSERT INTO accounts VALUES ($1,$2,1,$3,false,0,'\\x01',0)",
                [
                    key_bytes.clone().into(),
                    owner_bytes.clone().into(),
                    slot.into(),
                ],
            ))
            .await
            .unwrap();
        }
        let map = AccountOwnerMap {
            db: db.clone(),
            query_timeout: Duration::from_secs(10),
            accounts: Some(Arc::new(RwLock::new(HashMap::new()))),
            ..Default::default()
        };
        map.upsert_account(&key_bytes, &owner_bytes, 100);
        map.upsert_account(&key_bytes, &owner_bytes, 105);
        assert_eq!(
            map.save_closed_accounts(vec![key_bytes.clone()], 103)
                .await
                .unwrap()
                .rows_affected(),
            0
        );
        assert_eq!(map.get_owner(&key), Some(owner));
        assert!(map.account_to_be_deleted(&key_bytes, &owner_bytes, 106, false));
        assert_eq!(
            map.save_closed_accounts(vec![key_bytes.clone()], 106)
                .await
                .unwrap()
                .rows_affected(),
            1
        );
        assert_eq!(map.get_owner(&key), None);
        let row = db
            .query_one(Statement::from_string(
                sea_orm::DatabaseBackend::Postgres,
                "SELECT owner, lamports, data FROM accounts ORDER BY slot DESC LIMIT 1".to_string(),
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.try_get::<Vec<u8>>("", "owner").unwrap(), owner_bytes);
        assert_eq!(row.try_get::<i64>("", "lamports").unwrap(), 0);
        assert!(row.try_get::<Vec<u8>>("", "data").unwrap().is_empty());
        // A subsequent rediscovery and closure still works after the stale repair.
        map.upsert_account(&key_bytes, &owner_bytes, 107);
        assert_eq!(
            map.save_closed_accounts(vec![key_bytes], 108)
                .await
                .unwrap()
                .rows_affected(),
            1
        );
        bootstrap
            .execute_unprepared(&format!("DROP SCHEMA {schema} CASCADE"))
            .await
            .unwrap();
    }
}
