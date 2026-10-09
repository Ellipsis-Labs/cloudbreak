// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

pub use cloudbreak_core::{
    MigrationConfig, MigrationPgIndexesConfig, PgOwnerPartitionsConfig, TryLoadConfig,
};
pub use sea_orm_migration::prelude::*;
use std::sync::OnceLock;

mod m20220101_000001_create_slots_table;
pub(crate) mod m20250414_201255_create_accounts_table;
mod m20250714_055019_add_bs58_functions;
mod m20251008_080739_create_snapshot_accounts_table;
mod m20251021_222145_create_service_health_table;
mod m20260325_000000_drop_temp_tables;
mod m20260414_000000_create_indexer_filters_table;
mod m20260522_000000_create_environment_info_table;
mod m20260528_000000_create_epoch_stakes_table;
mod m20260703_000000_create_recent_blockhashes_table;
mod m20260709_000000_add_block_height_to_recent_blockhashes;
mod m20260711_000000_create_index_patterns_table;
mod m20260717_000000_create_supply_tables;
mod m20260808_000000_largest_accounts_record;
mod m20261009_000000_atomic_account_checkpoints;
mod m20261009_000001_token_mint_filters;

pub struct Migrator;

pub const CLOUDBREAK_MIGRATION_CONFIG_ENV: &str = "CLOUDBREAK_MIGRATION_CONFIG";

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20220101_000001_create_slots_table::Migration),
            Box::new(m20250414_201255_create_accounts_table::Migration),
            Box::new(m20250714_055019_add_bs58_functions::Migration),
            Box::new(m20251008_080739_create_snapshot_accounts_table::Migration),
            Box::new(m20251021_222145_create_service_health_table::Migration),
            Box::new(m20260325_000000_drop_temp_tables::Migration),
            Box::new(m20260414_000000_create_indexer_filters_table::Migration),
            Box::new(m20260522_000000_create_environment_info_table::Migration),
            Box::new(m20260528_000000_create_epoch_stakes_table::Migration),
            Box::new(m20260703_000000_create_recent_blockhashes_table::Migration),
            Box::new(m20260709_000000_add_block_height_to_recent_blockhashes::Migration),
            Box::new(m20260711_000000_create_index_patterns_table::Migration),
            Box::new(m20260717_000000_create_supply_tables::Migration),
            Box::new(m20260808_000000_largest_accounts_record::Migration),
            Box::new(m20261009_000000_atomic_account_checkpoints::Migration),
            Box::new(m20261009_000001_token_mint_filters::Migration),
        ]
    }
}

/// Cached migration config. Loaded once per process from the TOML file pointed at by
/// `CLOUDBREAK_MIGRATION_CONFIG`. Each migration that needs config just calls
/// `migration_config()`.
static MIGRATION_CONFIG: OnceLock<MigrationConfig> = OnceLock::new();

pub fn migration_config() -> &'static MigrationConfig {
    MIGRATION_CONFIG.get_or_init(|| {
        let path = std::env::var(CLOUDBREAK_MIGRATION_CONFIG_ENV).unwrap_or_else(|_| {
            panic!("{CLOUDBREAK_MIGRATION_CONFIG_ENV} must point to a TOML migration config file")
        });

        MigrationConfig::try_load(&path)
            .unwrap_or_else(|err| panic!("failed to load migration config from {path}: {err}"))
    })
}

/// Build the block that creates `table_name` with the requested partitioning shape.
///
/// Handles the four (hash, list) combinations:
/// - (false, false) → plain UNLOGGED table, PK is `(pubkey, slot)`.
/// - (true,  false) → `PARTITION BY HASH (owner)` with `hash_partition_count` buckets, PK is `(owner, pubkey, slot)`.
/// - (false, true ) → `PARTITION BY LIST (owner)` with per-program partitions and a plain (non-partitioned) `_default` catch-all.
/// - (true,  true ) → `PARTITION BY LIST (owner)` whose `_default` is further `PARTITION BY HASH (owner)`.
pub fn build_create_table_sql(table_name: &str, cfg: &PgOwnerPartitionsConfig) -> String {
    let columns = columns_sql();
    let primary_key = if cfg.is_owner_partitioned() {
        "PRIMARY KEY (owner, pubkey, slot)"
    } else {
        "PRIMARY KEY (pubkey, slot)"
    };

    match (cfg.hash_partitions, cfg.list_partitions) {
        (false, false) => format!(
            r#"
            CREATE UNLOGGED TABLE IF NOT EXISTS {table_name} (
                {columns},
                {primary_key}
            );
            "#
        ),
        (true, false) => {
            let hash_partitions =
                hash_partition_block(table_name, table_name, cfg.hash_partition_count);
            format!(
                r#"
                CREATE UNLOGGED TABLE IF NOT EXISTS {table_name} (
                    {columns},
                    {primary_key}
                ) PARTITION BY HASH (owner);

                {hash_partitions}
                "#
            )
        }
        (false, true) => {
            let list_partitions =
                list_partition_block(table_name, &cfg.programs_for_list_partition);
            format!(
                r#"
                CREATE UNLOGGED TABLE IF NOT EXISTS {table_name} (
                    {columns},
                    {primary_key}
                ) PARTITION BY LIST (owner);

                {list_partitions}

                CREATE UNLOGGED TABLE {table_name}_default PARTITION OF {table_name} DEFAULT;
                "#
            )
        }
        (true, true) => {
            let list_partitions =
                list_partition_block(table_name, &cfg.programs_for_list_partition);
            let default_table = format!("{table_name}_default");
            let hash_partitions =
                hash_partition_block(table_name, &default_table, cfg.hash_partition_count);
            format!(
                r#"
                CREATE UNLOGGED TABLE IF NOT EXISTS {table_name} (
                    {columns},
                    {primary_key}
                ) PARTITION BY LIST (owner);

                {list_partitions}

                CREATE UNLOGGED TABLE {default_table} PARTITION OF {table_name} DEFAULT
                    PARTITION BY HASH (owner);

                {hash_partitions}
                "#
            )
        }
    }
}

fn columns_sql() -> &'static str {
    r#"pubkey BYTEA NOT NULL,
            owner BYTEA NOT NULL,
            lamports BIGINT NOT NULL,
            slot BIGINT NOT NULL,
            executable BOOLEAN NOT NULL,
            rent_epoch NUMERIC(20, 0) NOT NULL,
            data BYTEA NOT NULL,
            write_version BIGINT NOT NULL,
            updated_on TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            txn_signature BYTEA,
            token_mint BYTEA GENERATED ALWAYS AS (SUBSTRING(data FROM 1 FOR 32)) STORED,
            token_owner BYTEA GENERATED ALWAYS AS (SUBSTRING(data FROM 33 FOR 32)) STORED"#
}

fn list_partition_block(table_name: &str, programs: &[cloudbreak_core::PubkeyDef]) -> String {
    programs
        .iter()
        .map(|program| {
            let pk = program.0;
            format!(
                r#"CREATE UNLOGGED TABLE {table_name}_{program_name} PARTITION OF {table_name} FOR VALUES IN ('\x{program_hex}');"#,
                program_name = pk,
                program_hex = hex::encode(pk.to_bytes()),
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn hash_partition_block(table_name: &str, parent_table: &str, num_partitions: u32) -> String {
    format!(
        r#"
        DO $$
        DECLARE
            num_partitions INTEGER := {num_partitions};
        BEGIN
            FOR i IN 0..(num_partitions - 1) LOOP
                EXECUTE format(
                    'CREATE UNLOGGED TABLE {table_name}_p%1$s PARTITION OF {parent_table} FOR VALUES WITH (MODULUS {num_partitions}, REMAINDER %1$s)',
                    i
                );
            END LOOP;
        END $$;
        "#
    )
}

#[cfg(test)]
mod compatibility_tests {
    use super::*;
    use cloudbreak_core::{AccountSelectorConfig, EnvironmentInfo, PubkeyDef};
    use sea_orm_migration::sea_orm::{DatabaseBackend, Statement};
    use solana_pubkey::Pubkey;

    #[tokio::test]
    #[ignore = "requires disposable PostgreSQL via CLOUDBREAK_TEST_DATABASE_URL"]
    async fn compatibility_checkpoint_migration_upgrade_and_rollback_preserve_legacy_data() {
        let url = std::env::var("CLOUDBREAK_TEST_DATABASE_URL").unwrap();
        let bootstrap = sea_orm::Database::connect(&url).await.unwrap();
        let schema = format!("migration_compat_{}", std::process::id());
        bootstrap
            .execute_unprepared(&format!("CREATE SCHEMA {schema}"))
            .await
            .unwrap();
        let mut options = sea_orm::ConnectOptions::new(url);
        options
            .max_connections(1)
            .set_schema_search_path(schema.clone());
        let db = sea_orm::Database::connect(options).await.unwrap();
        db.execute_unprepared("CREATE TABLE environment_info (id int PRIMARY KEY, mode text NOT NULL DEFAULT 'exclude', programs text NOT NULL DEFAULT '', solana_version text); CREATE TABLE accounts (pubkey bytea PRIMARY KEY, data bytea); INSERT INTO accounts VALUES ('\\x01','\\x0203');").await.unwrap();
        let program = PubkeyDef(Pubkey::new_from_array([1; 32]));
        let vault = PubkeyDef(Pubkey::new_from_array([2; 32]));
        let legacy = AccountSelectorConfig {
            include: vec![program.clone()],
            ..Default::default()
        };
        EnvironmentInfo::upsert_filters(&db, &legacy).await.unwrap();
        assert!(
            EnvironmentInfo::load_filters(&db)
                .await
                .unwrap()
                .accounts
                .is_empty()
        );
        let exact = AccountSelectorConfig {
            accounts: vec![vault],
            ..legacy.clone()
        };
        assert!(EnvironmentInfo::upsert_filters(&db, &exact).await.is_err());
        let manager = SchemaManager::new(&db);
        let migration = m20261009_000000_atomic_account_checkpoints::Migration;
        migration.up(&manager).await.unwrap();
        EnvironmentInfo::upsert_filters(&db, &exact).await.unwrap();
        assert_eq!(
            EnvironmentInfo::load_filters(&db)
                .await
                .unwrap()
                .accounts
                .len(),
            1
        );
        let mint_filter = cloudbreak_core::TokenMintFilter {
            mint: PubkeyDef(Pubkey::new_from_array([3; 32])),
            token_program: PubkeyDef(cloudbreak_core::modules::token_mint_filter::TOKEN_PROGRAM_ID),
        };
        let mint_selected = AccountSelectorConfig {
            token_mint_filters: vec![mint_filter.clone()],
            ..exact.clone()
        };
        assert!(
            EnvironmentInfo::upsert_filters(&db, &mint_selected)
                .await
                .is_err()
        );
        let mint_migration = m20261009_000001_token_mint_filters::Migration;
        mint_migration.up(&manager).await.unwrap();
        EnvironmentInfo::upsert_filters(&db, &mint_selected)
            .await
            .unwrap();
        let loaded = EnvironmentInfo::load_filters(&db).await.unwrap();
        assert_eq!(loaded.token_mint_filters, vec![mint_filter]);
        assert_eq!(loaded.accounts, exact.accounts);
        assert!(mint_migration.down(&manager).await.is_err());
        assert!(migration.down(&manager).await.is_err()); // never discard active filter metadata
        EnvironmentInfo::upsert_filters(&db, &legacy).await.unwrap();
        assert!(
            EnvironmentInfo::load_filters(&db)
                .await
                .unwrap()
                .accounts
                .is_empty()
        );
        // The pre-change writer/reader SQL still works after the additive migration.
        db.execute(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "INSERT INTO environment_info (id, mode, programs) VALUES (1, 'include', $1) ON CONFLICT (id) DO UPDATE SET mode = EXCLUDED.mode, programs = EXCLUDED.programs",
            [program.0.to_string().into()],
        )).await.unwrap();
        assert!(
            db.query_one(Statement::from_string(
                DatabaseBackend::Postgres,
                "SELECT mode, programs FROM environment_info WHERE id=1".to_string()
            ))
            .await
            .unwrap()
            .is_some()
        );
        mint_migration.down(&manager).await.unwrap();
        migration.down(&manager).await.unwrap();
        EnvironmentInfo::upsert_filters(&db, &legacy).await.unwrap();
        let filters = EnvironmentInfo::load_filters(&db).await.unwrap();
        assert!(filters.accounts.is_empty());
        assert_eq!(filters.include[0].0, program.0);
        let row = db
            .query_one(Statement::from_string(
                DatabaseBackend::Postgres,
                "SELECT data FROM accounts WHERE pubkey='\\x01'".to_string(),
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.try_get::<Vec<u8>>("", "data").unwrap(), vec![2, 3]);
        db.close().await.unwrap();
        bootstrap
            .execute_unprepared(&format!("DROP SCHEMA {schema} CASCADE"))
            .await
            .unwrap();
    }
}
