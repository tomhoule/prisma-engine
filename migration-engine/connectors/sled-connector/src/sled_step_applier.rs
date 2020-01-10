use crate::*;
use migration_connector::*;
use crate::schema::*;

pub(crate) struct SledMigrationStepApplier<'a> {
    pub(crate) connector: &'a SledConnector,
    pub(crate) table: Table<crate::schema::TableMetadata>,
}


#[async_trait::async_trait]
impl migration_connector::DatabaseMigrationStepApplier<SledMigration> for SledMigrationStepApplier<'_> {
    async fn apply_step(&self, database_migration: &SledMigration, step: usize) -> ConnectorResult<bool> {
        for step in &database_migration.steps {
            match step {
                SledMigrationStep::CreateTable(table) => {
                    // let table = TableMetadata { name: name.clone(), columns: Vec::new() };
                    self.table.persist(table.name.as_bytes(), &table).unwrap();

                }
                SledMigrationStep::DeleteTable(name) => {
                    self.table.delete(name.as_bytes()).unwrap();
                }
            }
        }

        Ok(false)
    }

    async fn unapply_step(&self, database_migration: &SledMigration, step: usize) -> ConnectorResult<bool> {
        unimplemented!()
    }

    fn render_steps_pretty(&self, database_migration: &SledMigration) -> ConnectorResult<Vec<serde_json::Value>> {
        Ok(Vec::new())
    }
}
