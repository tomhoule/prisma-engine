mod schema;
mod sled_migration_persistence;
mod sled_step_applier;

use datamodel::Datamodel;
use migration_connector::{
    ConnectorResult, DatabaseMigrationInferrer, DatabaseMigrationStepApplier, DestructiveChangeDiagnostics,
    DestructiveChangesChecker, MigrationPersistence, MigrationStep,
};
use serde::{Deserialize, Serialize};
use sled_migration_persistence::*;
use sled_step_applier::*;
use std::collections::BTreeMap;

#[derive(Debug)]
struct Database {
    system_tables: SystemTables,
}

impl Database {
    fn describe(&self) -> schema::Schema {
        let tables = self
            .system_tables
            .tables
            .tree
            .iter()
            .map(|(_, v)| unimplemented!())
            .collect();

        schema::Schema { tables }
    }
}

/// The tables
#[derive(Debug)]
struct SystemTables {
    tables: SystemTable,
    columns: SystemTable,
}

#[derive(Debug)]
struct SystemTable {
    name: &'static str,
    schema: schema::TableMetadata,
    tree: sled::Tree,
}

impl SystemTable {
    fn insert_by_names<'a>(
        &self,
        id: &u8,
        values: impl Iterator<Item = (&'a str, serde_json::Value)>,
    ) -> Result<(), anyhow::Error> {
        let mut value = Vec::new();

        for value in values {
            todo!()
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct SledConnector {
    db: sled::Db,
    database_file_path: String,
    system_tables: SystemTables,
}

impl SledConnector {
    pub fn new(database_file_path: String, db: sled::Db) -> Self {
        SledConnector {
            database_file_path,
            db,
            system_tables: SystemTables {
                columns: db.open_tree("_Columns").unwrap(),
                tables: db.open_tree("_Tables").unwrap(),
            },
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
enum SledMigrationStep {
    CreateTable(schema::TableMetadata),
    DeleteTable(String),
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SledMigration {
    steps: Vec<SledMigrationStep>,
}

impl migration_connector::DatabaseMigrationMarker for SledMigration {
    fn serialize(&self) -> serde_json::Value {
        serde_json::to_value(&self).unwrap()
    }
}

#[async_trait::async_trait]
impl migration_connector::MigrationConnector for SledConnector {
    type DatabaseMigration = SledMigration;

    fn connector_type(&self) -> &'static str {
        "sled"
    }

    async fn create_database(&self, _name: &str) -> ConnectorResult<()> {
        Ok(())
    }

    async fn reset(&self) -> ConnectorResult<()> {
        Ok(())
    }

    async fn initialize(&self) -> ConnectorResult<()> {
        Ok(())
    }

    fn migration_persistence<'a>(&'a self) -> Box<dyn MigrationPersistence + 'a> {
        Box::new(SledMigrationPersistence {
            connector: self,
            tree: self.db.open_tree("_Migrations").unwrap(),
        })
    }

    fn database_migration_inferrer<'a>(&'a self) -> Box<dyn DatabaseMigrationInferrer<SledMigration> + 'a> {
        Box::new(SledMigrationInferrer { connector: self })
    }

    fn database_migration_step_applier<'a>(&'a self) -> Box<dyn DatabaseMigrationStepApplier<SledMigration> + 'a> {
        Box::new(SledMigrationStepApplier {
            connector: self,
            table: Table {
                name: "_Tables",
                tree: self.db.open_tree("_Tables").unwrap(),
                _record_type: std::marker::PhantomData,
            },
        })
    }

    fn destructive_changes_checker<'a>(&'a self) -> Box<dyn DestructiveChangesChecker<SledMigration> + 'a> {
        Box::new(SledDestructiveChangesChecker)
    }

    fn deserialize_database_migration(&self, _json: serde_json::Value) -> SledMigration {
        unimplemented!()
    }
}

struct SledMigrationInferrer<'a> {
    connector: &'a SledConnector,
}

#[async_trait::async_trait]
impl migration_connector::DatabaseMigrationInferrer<SledMigration> for SledMigrationInferrer<'_> {
    async fn infer(
        &self,
        previous: &Datamodel,
        next: &Datamodel,
        steps: &[MigrationStep],
    ) -> ConnectorResult<SledMigration> {
        todo!()
        // let current_schema: std::collections::BTreeMap<String, schema::TableMetadata> =
        //     self.connector.tables.load_all().collect::<Result<_, _>>().unwrap();
        // let mut next_schema = current_schema.clone();

        // for step in steps {
        //     match step {
        //         MigrationStep::CreateModel(create_model) => {
        //             let table = schema::TableMetadata {
        //                 name: create_model.model.clone(),
        //                 columns: Vec::new(),
        //             };
        //             next_schema.insert(create_model.model.clone(), table);
        //         }
        //         MigrationStep::DeleteModel(delete_model) => {
        //             next_schema.remove(&delete_model.model);
        //         }
        //         _ => (),
        //     }
        // }

        // let sled_steps = diff_schemas(&current_schema, next_schema);

        // Ok(SledMigration { steps: sled_steps })
    }

    async fn infer_from_datamodels(
        &self,
        previous: &Datamodel,
        next: &Datamodel,
        steps: &[MigrationStep],
    ) -> ConnectorResult<SledMigration> {
        let mut steps = Vec::new();
        for model in next.models() {
            todo!()
            // steps.push(SledMigrationStep::CreateTable(model.name.clone()));
        }

        Ok(SledMigration { steps })
    }
}

struct SledDestructiveChangesChecker;

#[async_trait::async_trait]
impl migration_connector::DestructiveChangesChecker<SledMigration> for SledDestructiveChangesChecker {
    async fn check(&self, database_migration: &SledMigration) -> ConnectorResult<DestructiveChangeDiagnostics> {
        Ok(DestructiveChangeDiagnostics::new())
    }
}

#[derive(Clone)]
struct Table<R> {
    name: &'static str,
    tree: sled::Tree,
    _record_type: std::marker::PhantomData<R>,
}

impl<R> Table<R>
where
    R: Serialize + serde::de::DeserializeOwned,
{
    pub(crate) fn persist(&self, id: &[u8], record: &R) -> Result<(), anyhow::Error> {
        let bytes = serde_json::to_vec(record)?;
        self.tree.insert(id, bytes)?;
        Ok(())
    }

    pub(crate) fn get(&self, id: &[u8]) -> Result<R, anyhow::Error> {
        let bytes = self.tree.get(id)?.unwrap();
        let record = serde_json::from_slice(&bytes)?;

        Ok(record)
    }

    pub(crate) fn delete(&self, id: &[u8]) -> Result<(), anyhow::Error> {
        self.tree.remove(id).map_err(From::from).map(drop)
    }

    pub(crate) fn load_all(&self) -> impl Iterator<Item = Result<(String, R), anyhow::Error>> {
        self.tree
            .iter()
            .map(|(k, v)| Ok((k.into(), serde_json::from_slice(&v)?)))
    }
}

fn diff_schemas(
    previous: &BTreeMap<String, schema::TableMetadata>,
    next: &BTreeMap<String, schema::TableMetadata>,
) -> Vec<SledMigrationStep> {
    unimplemented!()
}
