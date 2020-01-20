mod database_migration_inferrer;
mod database_migration_step_applier;
mod destructive_changes_checker;
mod migration_persistence;

pub use sled_wrapper;

use migration_connector::*;
use serde::{Deserialize, Serialize};
use sled_wrapper::migrations::SledMigration as SledMigrationStep;

#[derive(Debug)]
pub struct SledMigrationConnector {
    connection: sled_wrapper::Connection,
}

impl SledMigrationConnector {
    pub fn new(file_path: std::path::PathBuf) -> Result<Self, anyhow::Error> {
        tracing::debug!("Starting sled migration connector at {:?}", &file_path);
        Ok(SledMigrationConnector {
            connection: sled_wrapper::Connection::new(file_path)?,
        })
    }

    pub fn get_schema(&self) -> anyhow::Result<sled_wrapper::schema::Schema> {
        Ok(self.connection.get_schema()?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SledMigration {
    steps: Vec<SledMigrationStep>,
}

impl migration_connector::DatabaseMigrationMarker for SledMigration {
    fn serialize(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap()
    }
}

#[async_trait::async_trait]
impl migration_connector::MigrationConnector for SledMigrationConnector {
    type DatabaseMigration = SledMigration;

    async fn initialize(&self) -> ConnectorResult<()> {
        Ok(())
    }

    async fn create_database(&self, db_name: &str) -> ConnectorResult<()> {
        Ok(())
    }

    async fn reset(&self) -> ConnectorResult<()> {
        Ok(())
    }

    fn deserialize_database_migration(&self, mig: serde_json::Value) -> Self::DatabaseMigration {
        serde_json::from_value(mig).unwrap()
    }

    fn connector_type(&self) -> &'static str {
        "sled"
    }

    fn destructive_changes_checker(&self) -> Box<dyn DestructiveChangesChecker<SledMigration>> {
        Box::new(destructive_changes_checker::SledDestructiveChangesChecker)
    }

    fn migration_persistence<'a>(&'a self) -> Box<dyn MigrationPersistence + 'a> {
        Box::new(migration_persistence::SledMigrationPersistence { connector: self })
    }

    fn database_migration_inferrer(&self) -> Box<dyn DatabaseMigrationInferrer<SledMigration>> {
        Box::new(database_migration_inferrer::SledDatabaseMigrationInferrer)
    }

    fn database_migration_step_applier<'a>(&'a self) -> Box<dyn DatabaseMigrationStepApplier<SledMigration> + 'a> {
        Box::new(database_migration_step_applier::SledDatabaseMigrationStepApplier { connector: self })
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
