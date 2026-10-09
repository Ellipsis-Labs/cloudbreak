//! Extension checkpoint publication, kept out of the shared slot-writing path.
use std::time::Duration;

use cloudbreak_core::IndexConfig;
use sea_orm::{
    ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement, TransactionTrait, Value,
};
use yellowstone_grpc_proto::{geyser::CommitmentLevel, prelude::UnixTimestamp};

use crate::{db_queries, metrics};

pub async fn publish_confirmed_slot(
    slot: u64,
    block_time: Option<UnixTimestamp>,
    blockhash: &str,
    healthy: bool,
    transaction_count: Option<u64>,
    db: &DatabaseConnection,
    config: &IndexConfig,
) -> bool {
    let publish = async {
        let txn = db.begin().await?;
        txn.execute(db_queries::slot_statement(
            slot,
            block_time,
            Some(blockhash),
            CommitmentLevel::Confirmed,
            healthy,
        ))
        .await?;
        // Repaired blocks have no trustworthy transaction count or blockhash.
        if let Some(count) = transaction_count.filter(|_| !blockhash.is_empty()) {
            let count = i64::try_from(count)
                .map_err(|_| sea_orm::DbErr::Custom("Transaction count exceeds BIGINT".into()))?;
            txn.execute(Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                "INSERT INTO atomic_account_checkpoint (id, slot, transaction_count, blockhash) VALUES (1, $1, $2, $3) ON CONFLICT (id) DO UPDATE SET slot = EXCLUDED.slot, transaction_count = EXCLUDED.transaction_count, blockhash = EXCLUDED.blockhash WHERE EXCLUDED.slot > atomic_account_checkpoint.slot",
                [Value::from(slot as i64), Value::from(count), Value::from(blockhash)],
            )).await?;
        }
        txn.commit().await
    };
    match tokio::time::timeout(
        Duration::from_secs(config.database.finalize_slot_queries_timeout),
        publish,
    )
    .await
    {
        Ok(Ok(())) => true,
        error => {
            tracing::error!(
                "Failed to publish confirmed account checkpoint for slot {slot}: {error:?}"
            );
            metrics::increment_db_errors();
            false
        }
    }
}

#[cfg(test)]
mod compatibility_tests {
    use super::*;
    use cloudbreak_core::TryLoadConfig;

    #[tokio::test]
    #[ignore = "requires disposable PostgreSQL via CLOUDBREAK_TEST_DATABASE_URL"]
    async fn compatibility_legacy_slot_writer_and_atomic_checkpoint_failures() {
        // Fault injection must not trigger the production process-exit threshold.
        let _ = metrics::DB_ERRORS_THRESHOLD.set(0.0);
        let url = std::env::var("CLOUDBREAK_TEST_DATABASE_URL").unwrap();
        let bootstrap = sea_orm::Database::connect(&url).await.unwrap();
        let schema = format!("checkpoint_compat_{}", std::process::id());
        bootstrap
            .execute_unprepared(&format!("CREATE SCHEMA {schema}"))
            .await
            .unwrap();
        let mut options = sea_orm::ConnectOptions::new(url);
        options
            .max_connections(1)
            .set_schema_search_path(schema.clone());
        let db = sea_orm::Database::connect(options).await.unwrap();
        db.execute_unprepared("CREATE TABLE slots (slot bigint, commitment int PRIMARY KEY, block_time bigint, health bool, blockhash text);").await.unwrap();
        let config = IndexConfig::try_load(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../example.cloudbreak.index.toml"
        ))
        .unwrap();
        assert!(!config.phoenix_accounts.enabled);
        assert!(
            db_queries::insert_slot(
                100,
                None,
                Some("hash100"),
                CommitmentLevel::Confirmed,
                true,
                &db,
                &config
            )
            .await
        );
        // Feature use without the migration rolls back the slot update too.
        assert!(!publish_confirmed_slot(101, None, "hash101", true, Some(3), &db, &config).await);
        let current_slot = || async {
            db.query_one(Statement::from_string(
                DatabaseBackend::Postgres,
                "SELECT slot FROM slots WHERE commitment=1".to_string(),
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get::<i64>("", "slot")
            .unwrap()
        };
        assert_eq!(current_slot().await, 100);
        db.execute_unprepared("CREATE TABLE atomic_account_checkpoint (id int PRIMARY KEY, slot bigint CHECK(slot<105), transaction_count bigint CHECK(transaction_count>=0), blockhash text);").await.unwrap();
        assert!(publish_confirmed_slot(103, None, "hash103", true, Some(7), &db, &config).await);
        assert!(publish_confirmed_slot(102, None, "hash102", true, Some(4), &db, &config).await);
        assert_eq!(current_slot().await, 103);
        assert!(publish_confirmed_slot(104, None, "", true, None, &db, &config).await);
        let checkpoint = db
            .query_one(Statement::from_string(
                DatabaseBackend::Postgres,
                "SELECT slot, transaction_count FROM atomic_account_checkpoint WHERE id=1"
                    .to_string(),
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(checkpoint.try_get::<i64>("", "slot").unwrap(), 103);
        assert_eq!(
            checkpoint.try_get::<i64>("", "transaction_count").unwrap(),
            7
        );
        assert!(!publish_confirmed_slot(105, None, "hash105", true, Some(9), &db, &config).await);
        assert_eq!(current_slot().await, 104);
        assert!(
            !publish_confirmed_slot(105, None, "hash105", true, Some(u64::MAX), &db, &config).await
        );
        assert_eq!(current_slot().await, 104);
        assert!(
            db_queries::insert_slot(
                100,
                None,
                None,
                CommitmentLevel::Finalized,
                true,
                &db,
                &config
            )
            .await
        );
        db.close().await.unwrap();
        bootstrap
            .execute_unprepared(&format!("DROP SCHEMA {schema} CASCADE"))
            .await
            .unwrap();
    }
}
