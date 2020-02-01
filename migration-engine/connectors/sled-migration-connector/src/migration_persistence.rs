use migration_connector::*;

pub struct SledMigrationPersistence<'a> {
    pub(super) connector: &'a super::SledMigrationConnector,
}

impl SledMigrationPersistence<'_> {
    fn conn(&self) -> &sled_wrapper::Connection {
        &self.connector.connection
    }
}

#[async_trait::async_trait]
impl MigrationPersistence for SledMigrationPersistence<'_> {
    async fn init(&self) -> ConnectorResult<()> {
        let conn = self.conn();
        let mut schema = conn.get_schema().unwrap();
        debug_assert!(schema.tables.iter().find(|table| table.name == "_Migrations").is_none());

        schema.tables.push(migration_table());

        conn.persist_schema(&schema).unwrap();

        Ok(())
    }

    async fn reset(&self) -> ConnectorResult<()> {
        Ok(())
    }

    async fn last(&self) -> ConnectorResult<Option<Migration>> {
        Ok(None)
    }

    async fn by_name(&self, name: &str) -> ConnectorResult<Option<Migration>> {
        Ok(None)
    }

    async fn load_all(&self) -> ConnectorResult<Vec<Migration>> {
        self.conn()
            .scan("_Migrations")
            .map_err(|err| ConnectorError::from_kind(ErrorKind::QueryError(err)))?
            .map(|result| result.map_err(|err| ConnectorError::from_kind(ErrorKind::QueryError(err))))
            .collect::<Result<Vec<_>, _>>()
    }

    async fn create(&self, migration: Migration) -> ConnectorResult<Migration> {
        Ok(migration)
    }

    async fn update(&self, params: &MigrationUpdateParams) -> ConnectorResult<()> {
        Ok(())
    }
}

fn migration_table() -> sled_wrapper::schema::Table {
    use sled_wrapper::schema;

    schema::Table {
        name: "_Migrations".to_owned(),
        id_columns: vec![schema::Column {
            name: "revision".to_owned(),
            required: true,
            r#type: schema::ColumnType::I32,
        }],
        columns: vec![
            schema::Column {
                name: "name".to_owned(),
                r#type: schema::ColumnType::String,
                required: true,
            },
            schema::Column {
                name: "datamodel".to_owned(),
                r#type: schema::ColumnType::String,
                required: true,
            },
            schema::Column {
                name: "datamodel".to_owned(),
                r#type: schema::ColumnType::String,
                required: true,
            },
            schema::Column {
                name: "started_at".to_owned(),
                r#type: schema::ColumnType::String,
                required: true,
            },
            schema::Column {
                name: "finished_at".to_owned(),
                r#type: schema::ColumnType::String,
                required: true,
            },
        ],
    }
}
