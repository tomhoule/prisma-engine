pub use super::misc_helpers::TestResult;
pub use super::test_api::*;

use migration_core::api::MigrationApi;
use sled_migration_connector::{sled_wrapper::schema, SledMigration, SledMigrationConnector};

pub struct TestApi {
    name: &'static str,
    engine: MigrationApi<SledMigrationConnector, SledMigration>,
}

impl TestApi {
    pub fn infer_apply<'a>(&'a self, dm: &'a str) -> InferApply<'a> {
        InferApply::new(&self.engine, dm)
    }

    pub fn assert_schema(&self) -> anyhow::Result<SchemaAssertion> {
        let schema = self.engine.connector().get_schema()?;
        dbg!(&schema);
        Ok(SchemaAssertion(schema))
    }

    pub fn get_schema(&self) -> anyhow::Result<schema::Schema> {
        self.engine.connector().get_schema()
    }
}

pub struct SchemaAssertion(schema::Schema);

type AssertionResult<T> = anyhow::Result<T>;

impl SchemaAssertion {
    pub fn assert_has_table(self, table_name: &str) -> AssertionResult<Self> {
        let has_table = self.0.tables.iter().any(|table| table.name == table_name);
        anyhow::ensure!(has_table, "Assertion failed. Could not find table {}", table_name);
        Ok(self)
    }

    pub fn assert_does_not_have_table(self, table_name: &str) -> AssertionResult<Self> {
        if self.0.tables.iter().any(|table| table.name == table_name) {
            anyhow::bail!("Assertion failed: table {} is in the schema", table_name);
        }

        Ok(self)
    }
}

pub async fn test_api(name: &'static str) -> TestApi {
    let root = test_setup::server_root();
    let file_path = std::path::Path::new(&root).join("db").join(name);
    std::fs::remove_dir_all(&file_path).ok();
    let connector = SledMigrationConnector::new(file_path).unwrap();
    let engine = MigrationApi::new(connector).await.unwrap();
    TestApi { name, engine }
}
