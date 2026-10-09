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
            "ALTER TABLE environment_info ADD COLUMN token_mint_filters JSONB NOT NULL DEFAULT '[]' CHECK (jsonb_typeof(token_mint_filters) = 'array');"
        ).await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let row = manager.get_connection().query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT EXISTS (SELECT 1 FROM environment_info WHERE token_mint_filters <> '[]'::jsonb) AS enabled".to_string(),
        )).await?.ok_or_else(|| DbErr::Custom("Missing filter metadata".into()))?;
        if row.try_get::<bool>("", "enabled")? {
            return Err(DbErr::Custom(
                "Clear token-mint filters before rolling back their migration".into(),
            ));
        }
        manager
            .get_connection()
            .execute_unprepared("ALTER TABLE environment_info DROP COLUMN token_mint_filters;")
            .await?;
        Ok(())
    }
}
