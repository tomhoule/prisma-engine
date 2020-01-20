use crate::SledMigration;
use migration_connector::*;

pub struct SledDatabaseMigrationStepApplier<'a> {
    pub(super) connector: &'a crate::SledMigrationConnector,
}

#[async_trait::async_trait]
impl DatabaseMigrationStepApplier<SledMigration> for SledDatabaseMigrationStepApplier<'_> {
    async fn apply_step(&self, migration: &SledMigration, step: usize) -> ConnectorResult<bool> {
        let step = migration.steps.get(step);
        let mut schema = self.connector.get_schema().expect("get schema");

        if let Some(step) = step {
            sled_wrapper::migrations::apply(&mut schema, step);
            self.connector.connection.persist_schema(&schema).unwrap();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn unapply_step(&self, migration: &SledMigration, step: usize) -> ConnectorResult<bool> {
        Ok(false)
    }

    fn render_steps_pretty(&self, migration: &SledMigration) -> ConnectorResult<Vec<serde_json::Value>> {
        Ok(Vec::new())
    }
}
