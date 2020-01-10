use crate::SledConnector;
use migration_connector::*;

pub(crate) struct SledMigrationPersistence<'a> {
    pub(crate) connector: &'a SledConnector,
    pub(crate) tree: sled::Tree,
}

#[async_trait::async_trait]
impl migration_connector::MigrationPersistence for SledMigrationPersistence<'_> {
    async fn init(&self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn reset(&self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn last(&self) -> Result<Option<Migration>, ConnectorError> {
        let all = self.load_all().await?;

        Ok(all.into_iter().last())
    }

    async fn by_name(&self, name: &str) -> Result<Option<Migration>, ConnectorError> {
        Ok(self.tree.get(name.as_bytes()).unwrap().map(|bytes| serde_json::from_slice(&bytes).unwrap()))
    }

    async fn load_all(&self) -> Result<Vec<Migration>, ConnectorError> {
        let mut migrations = Vec::new();


        for record in self.tree.iter() {
            let (_k, value) = record.unwrap();
            migrations.push(serde_json::from_slice(&value).unwrap());
        }


        Ok(migrations)
    }

    async fn create(&self, migration: Migration) -> Result<Migration, ConnectorError> {
        let bytes = serde_json::to_vec(&migration).unwrap();
        let id = migration.name.clone();
        self.tree.insert(id, bytes).unwrap();
        Ok(migration)
    }

    async fn update(&self, params: &MigrationUpdateParams) -> Result<(), ConnectorError> {
        let mut migration = self.by_name(&params.name).await?.unwrap();
        let MigrationUpdateParams {
            new_name,
            revision,
            status,
            applied,
            rolled_back,
            errors,
            finished_at,
            name,
        } = params;
        migration.name = new_name.clone();
        migration.revision = *revision;
        migration.status = *status;
        migration.applied = *applied;
        migration.rolled_back = *rolled_back;
        migration.errors = errors.clone();
        migration.finished_at = finished_at.clone();

        self.tree.insert(name.as_bytes(), serde_json::to_vec(&migration).unwrap()).unwrap();


        Ok(())
    }
}
