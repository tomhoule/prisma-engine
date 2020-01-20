use crate::{error::ConnectorError, steps::*};
use chrono::{DateTime, Utc};
use datamodel::{ast::SchemaAst, Datamodel};
use serde::{Deserialize, Serialize};

/// This trait is implemented by each connector. It provides a generic API to store and retrieve [Migration](struct.Migration.html) records.
#[async_trait::async_trait]
pub trait MigrationPersistence: Send + Sync {
    /// Initialize migration persistence state. E.g. create the migrations table in an SQL database.
    async fn init(&self) -> Result<(), ConnectorError>;

    /// Drop all persisted state.
    async fn reset(&self) -> Result<(), ConnectorError>;

    async fn current_datamodel_ast(&self) -> Result<SchemaAst, ConnectorError> {
        let ast = self
            .last()
            .await?
            .and_then(|m| datamodel::ast::parser::parse(&m.datamodel_string).ok())
            .unwrap_or_else(SchemaAst::empty);

        Ok(ast)
    }

    async fn last_non_watch_applied_migration(&self) -> Result<Option<Migration>, ConnectorError> {
        let migration =
            self.load_all().await?.into_iter().rev().find(|migration| {
                !migration.is_watch_migration() && migration.status == MigrationStatus::MigrationSuccess
            });

        Ok(migration)
    }

    async fn last_non_watch_migration(&self) -> Result<Option<Migration>, ConnectorError> {
        let mut all_migrations = self.load_all().await?;
        all_migrations.reverse();
        let migration = all_migrations.into_iter().find(|m| !m.is_watch_migration());

        Ok(migration)
    }

    /// Returns the last successful Migration.
    async fn last(&self) -> Result<Option<Migration>, ConnectorError>;

    /// Fetch a migration by name.
    async fn by_name(&self, name: &str) -> Result<Option<Migration>, ConnectorError>;

    /// This powers the listMigrations command.
    async fn load_all(&self) -> Result<Vec<Migration>, ConnectorError>;

    /// Load all current trailing watch migrations from Migration Event Log.
    async fn load_current_watch_migrations(&self) -> Result<Vec<Migration>, ConnectorError> {
        let mut all_migrations = self.load_all().await?;
        let mut result = Vec::new();
        // start to take all migrations from the back until we hit a migration that is not watch
        all_migrations.reverse();
        for migration in all_migrations {
            if migration.is_watch_migration() {
                result.push(migration);
            } else {
                break;
            }
        }
        // reverse the result so the migrations are in the right order again
        result.reverse();
        Ok(result)
    }

    /// Write the migration to the Migration table.
    async fn create(&self, migration: Migration) -> Result<Migration, ConnectorError>;

    /// Used by the MigrationApplier to write the progress of a [Migration](struct.Migration.html)
    /// into the database.
    async fn update(&self, params: &MigrationUpdateParams) -> Result<(), ConnectorError>;

    /// Returns whether the migration with the provided migration id has already been successfully applied.
    ///
    /// The default impl will load all migrations and scan for the provided migration id. Implementors are encouraged to implement this more efficiently.
    async fn migration_is_already_applied(&self, migration_id: &str) -> Result<bool, ConnectorError> {
        let migrations = self.load_all().await?;

        let already_applied = migrations
            .iter()
            .any(|migration| migration.status == MigrationStatus::MigrationSuccess && migration.name == migration_id);

        Ok(already_applied)
    }
}

/// The representation of a migration as persisted through [MigrationPersistence](trait.MigrationPersistence.html).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Migration {
    pub name: String,
    pub revision: usize,
    pub status: MigrationStatus,
    pub applied: usize,
    pub rolled_back: usize,
    pub datamodel_string: String,
    pub datamodel_steps: Vec<MigrationStep>,
    pub database_migration: serde_json::Value,
    pub errors: Vec<String>,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
}

impl Migration {
    pub fn parse_datamodel(&self) -> Datamodel {
        datamodel::parse_datamodel(&self.datamodel_string).unwrap()
    }

    pub fn parse_schema_ast(&self) -> SchemaAst {
        datamodel::parse_schema_ast(&self.datamodel_string).unwrap()
    }
}

/// Updates to be made to a persisted [Migration](struct.Migration.html).
#[derive(Debug, Clone)]
pub struct MigrationUpdateParams {
    pub name: String,
    pub new_name: String,
    pub revision: usize,
    pub status: MigrationStatus,
    pub applied: usize,
    pub rolled_back: usize,
    pub errors: Vec<String>,
    pub finished_at: Option<DateTime<Utc>>,
}

impl MigrationUpdateParams {
    pub fn mark_as_finished(&mut self) {
        self.status = MigrationStatus::MigrationSuccess;
        self.finished_at = Some(Migration::timestamp_without_nanos());
    }
}

pub trait IsWatchMigration {
    fn is_watch_migration(&self) -> bool;
}

impl Migration {
    pub fn new(name: String) -> Migration {
        Migration {
            name: name,
            revision: 0,
            status: MigrationStatus::Pending,
            datamodel_string: String::new(),
            applied: 0,
            rolled_back: 0,
            datamodel_steps: Vec::new(),
            database_migration: serde_json::to_value("{}").unwrap(),
            errors: Vec::new(),
            started_at: Self::timestamp_without_nanos(),
            finished_at: None,
        }
    }

    pub fn update_params(&self) -> MigrationUpdateParams {
        MigrationUpdateParams {
            name: self.name.clone(),
            new_name: self.name.clone(),
            revision: self.revision.clone(),
            status: self.status.clone(),
            applied: self.applied,
            rolled_back: self.rolled_back,
            errors: self.errors.clone(),
            finished_at: self.finished_at.clone(),
        }
    }

    // SQLite does not store nano precision. Therefore we cut it so we can assert equality in our tests.
    pub fn timestamp_without_nanos() -> DateTime<Utc> {
        let timestamp = Utc::now().timestamp_millis();
        let nsecs = ((timestamp % 1000) * 1_000_000) as u32;
        let secs = (timestamp / 1000) as i64;
        let naive = chrono::NaiveDateTime::from_timestamp(secs, nsecs);
        let datetime: DateTime<Utc> = DateTime::from_utc(naive, Utc);
        datetime
    }

    pub fn datamodel_ast(&self) -> SchemaAst {
        datamodel::ast::parser::parse(&self.datamodel_string)
            .ok()
            .unwrap_or_else(SchemaAst::empty)
    }

    pub fn datamodel(&self) -> Datamodel {
        datamodel::lift_ast(&self.datamodel_ast())
            .ok()
            .unwrap_or_else(Datamodel::empty)
    }
}

impl IsWatchMigration for Migration {
    fn is_watch_migration(&self) -> bool {
        self.name.starts_with("watch")
    }
}

#[derive(Debug, Serialize, PartialEq, Clone, Copy, Deserialize)]
pub enum MigrationStatus {
    Pending,
    MigrationInProgress,
    MigrationSuccess,
    MigrationFailure,
    RollingBack,
    RollbackSuccess,
    RollbackFailure,
}

impl MigrationStatus {
    pub fn code(&self) -> &str {
        match self {
            MigrationStatus::Pending => "Pending",
            MigrationStatus::MigrationInProgress => "MigrationInProgress",
            MigrationStatus::MigrationSuccess => "MigrationSuccess",
            MigrationStatus::MigrationFailure => "MigrationFailure",
            MigrationStatus::RollingBack => "RollingBack",
            MigrationStatus::RollbackSuccess => "RollbackSuccess",
            MigrationStatus::RollbackFailure => "RollbackFailure",
        }
    }

    pub fn from_str(s: String) -> MigrationStatus {
        match s.as_ref() {
            "Pending" => MigrationStatus::Pending,
            "MigrationInProgress" => MigrationStatus::MigrationInProgress,
            "MigrationSuccess" => MigrationStatus::MigrationSuccess,
            "MigrationFailure" => MigrationStatus::MigrationFailure,
            "RollingBack" => MigrationStatus::RollingBack,
            "RollbackSuccess" => MigrationStatus::RollbackSuccess,
            "RollbackFailure" => MigrationStatus::RollbackFailure,
            _ => panic!("MigrationStatus {:?} is not known", s),
        }
    }

    pub fn is_success(&self) -> bool {
        match self {
            MigrationStatus::MigrationSuccess => true,
            _ => false,
        }
    }

    pub fn is_pending(&self) -> bool {
        match self {
            MigrationStatus::Pending => true,
            _ => false,
        }
    }
}

/// A no-op implementor of [MigrationPersistence](trait.MigrationPersistence.html).
pub struct EmptyMigrationPersistence {}

#[async_trait::async_trait]
impl MigrationPersistence for EmptyMigrationPersistence {
    async fn init(&self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn reset(&self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn last(&self) -> Result<Option<Migration>, ConnectorError> {
        Ok(None)
    }

    async fn by_name(&self, _name: &str) -> Result<Option<Migration>, ConnectorError> {
        Ok(None)
    }

    async fn load_all(&self) -> Result<Vec<Migration>, ConnectorError> {
        Ok(Vec::new())
    }

    async fn create(&self, _migration: Migration) -> Result<Migration, ConnectorError> {
        unimplemented!("Not allowed on a EmptyMigrationPersistence")
    }

    async fn update(&self, _params: &MigrationUpdateParams) -> Result<(), ConnectorError> {
        unimplemented!("Not allowed on a EmptyMigrationPersistence")
    }

    async fn current_datamodel_ast(&self) -> Result<datamodel::ast::SchemaAst, ConnectorError> {
        Ok(datamodel::ast::SchemaAst { tops: Vec::new() })
    }
}
