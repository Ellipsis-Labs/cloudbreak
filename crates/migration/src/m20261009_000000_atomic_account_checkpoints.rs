use sea_orm_migration::{
    prelude::*,
    sea_orm::{DatabaseBackend, Statement},
};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager.get_connection().execute_unprepared(
            "ALTER TABLE environment_info ADD COLUMN accounts TEXT NOT NULL DEFAULT '';\n\
             CREATE TABLE atomic_account_checkpoint (\
             id INTEGER PRIMARY KEY CHECK (id = 1),\
             slot BIGINT NOT NULL, transaction_count BIGINT NOT NULL CHECK (transaction_count >= 0),\
             blockhash TEXT NOT NULL);"
        ).await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let row = manager.get_connection().query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT EXISTS (SELECT 1 FROM environment_info WHERE accounts <> '') AS exact_accounts_enabled".to_string(),
        )).await?.ok_or_else(|| DbErr::Custom("Missing filter metadata".into()))?;
        if row.try_get::<bool>("", "exact_accounts_enabled")? {
            return Err(DbErr::Custom(
                "Clear exact-account filters before rolling back the checkpoint migration".into(),
            ));
        }
        manager.get_connection().execute_unprepared(
            "DROP TABLE atomic_account_checkpoint; ALTER TABLE environment_info DROP COLUMN accounts;"
        ).await?;
        Ok(())
    }
}
