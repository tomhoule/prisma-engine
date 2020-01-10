use super::test_harness::*;
use crate::api::MigrationApi;
use sled_connector::*;

pub(crate) struct SledApi {
    api: MigrationApi<SledConnector, SledMigration>,
    db: sled::Db,
    db_name: &'static str,
}

impl SledApi {
    pub async fn new(db_name: &'static str) -> Self {
        let db_file_path = format!(
            "{}/db/l{}",
            std::env::var("SERVER_ROOT").expect("SERVER_ROOT").trim_end_matches('/'),
            db_name
        );
        std::fs::remove_dir_all(&db_file_path).unwrap();
        let db = sled::open(&db_file_path).unwrap();
        let connector = SledConnector::new(db_file_path, db.clone());
        let api = MigrationApi::new(connector).await.unwrap();
        SledApi { db_name, db, api }
    }

    pub fn infer_apply<'a>(&'a self, schema: &'a str) -> InferApply<'a> {
        InferApply::new(schema, &self.api)
    }

    pub fn assert_schema(&self) -> SchemaAssertions {
        SchemaAssertions(self.db.clone())
    }
}

type AssertionResult<T> = Result<T, anyhow::Error>;

pub(crate) struct SchemaAssertions(sled::Db);

impl SchemaAssertions {
    pub(crate) fn assert_tables_count(self, count: usize) -> AssertionResult<Self> {
        let tables_count = self
            .0
            .tree_names()
            .iter()
            .filter(|table| !table.starts_with("_".as_bytes()))
            .count();
        anyhow::ensure!(
            tables_count == count,
            "Expected {} tables, found {} ({:?})",
            count,
            tables_count,
            self.0
                .tree_names()
                .iter()
                .map(|ivec| String::from_utf8_lossy(ivec))
                .collect::<Vec<_>>()
        );

        Ok(self)
    }

    pub(crate) fn assert_has_table(self, table_name: &str) -> AssertionResult<Self> {
        let table_names = self.0.tree_names();
        assert!(table_names.iter().any(|name| name == table_name.as_bytes()));
        Ok(self)
    }
}

#[test_sled]
async fn sled_tables_get_created(api: &SledApi) -> TestResult {
    let dm = r#"
        model Human {
            id String @id
        }

        model Cat {
            id String @id
        }

    "#;

    api.infer_apply(dm).send().await?;

    api.assert_schema()
        .assert_tables_count(2)?
        .assert_has_table("Human")?
        .assert_has_table("Cat")
        .map(drop)
}

#[test_sled]
async fn sled_tables_get_deleted(api: &SledApi) -> TestResult {
    let dm1 = r#"
        model Human {
            id String @id
        }

        model Cat {
            id String @id
        }

    "#;

    api.infer_apply(dm1).send().await?;

    let dm2 = r#"
        model Human {
            id String @id
        }

    "#;

    api.infer_apply(dm2).send().await?;

    api.assert_schema()
        .assert_tables_count(1)?
        .assert_has_table("Human")
        .map(drop)
}
