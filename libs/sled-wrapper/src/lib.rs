mod database_value;
pub mod migrations;
mod record_reader;
mod record_writer;
pub mod schema;

pub use database_value::DatabaseValue;

use record_writer::write_row;
use schema::Schema;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::{collections::HashMap, sync::RwLock};

#[derive(Debug)]
pub struct Connection {
    db: sled::Db,
    system_table: sled::Tree,
    schema: RwLock<Schema>,
    db_path: std::path::PathBuf,
}

impl Connection {
    pub fn new(file_path: std::path::PathBuf) -> Result<Self, anyhow::Error> {
        let db = sled::open(&file_path)?;
        let system_table = db.open_tree("_System")?;
        let schema = RwLock::new(get_schema(&system_table)?);

        Ok(Connection {
            db,
            system_table,
            schema,
            db_path: file_path,
        })
    }

    pub fn migrate<F>(&mut self, migration_fn: F) -> anyhow::Result<()>
    where
        F: FnOnce(&mut schema::Schema),
    {
        let mut schema = self.schema.write().map_err(|_| anyhow::anyhow!("migration lock"))?;

        migration_fn(&mut *schema);
        self.persist_schema(&*schema)?;

        Ok(())
    }

    pub fn schema(&self) -> anyhow::Result<std::sync::RwLockReadGuard<Schema>> {
        self.schema
            .read()
            .map_err(|_err| anyhow::anyhow!("Schema lock is poisoned"))
    }

    pub fn reload_schema(&self) -> Result<(), anyhow::Error> {
        use std::ops::DerefMut;
        let schema = get_schema(&self.system_table)?;

        let mut old_schema = self
            .schema
            .write()
            .map_err(|_err| anyhow::anyhow!("Schema mutex is poisoned"))?;

        std::mem::replace(old_schema.deref_mut(), schema);

        Ok(())
    }

    pub fn persist_schema(&self, schema: &Schema) -> Result<(), anyhow::Error> {
        let bytes = serde_json::to_vec(schema)?;
        self.system_table.insert("schema", bytes.as_slice())?;

        Ok(())
    }

    pub fn insert<T: serde::Serialize>(&self, table_name: &str, value: &T) -> Result<(), anyhow::Error> {
        let schema = self.schema()?;
        let table = schema.get_table(table_name)?;
        let (key_bytes, value_bytes) = write_row(value, &table)?;

        let tree = self.db.open_tree(table_name)?;
        dbg!(&key_bytes, &value_bytes);
        tree.insert(key_bytes, value_bytes)?;

        Ok(())
    }

    pub fn get<'a, T: serde::de::DeserializeOwned>(
        &'a self,
        table: &str,
        id: impl Serialize,
    ) -> Result<Option<T>, anyhow::Error> {
        let tree = self.db.open_tree(table)?;
        let schema = self.schema()?;
        let table = schema.get_table(table)?;
        let mut id_bytes = Vec::new();

        id_bytes.push(0); // HAXXX

        let serializer = record_writer::ValueSerializer { buf: &mut id_bytes };

        id.serialize(serializer)?;

        let bytes = tree.get(dbg!(&id_bytes))?;

        let record = match bytes {
            Some(bytes) => {
                Some(record_reader::read_record::<T>(&id_bytes, &bytes, &table)?)
                // let values: Vec<DatabaseValue> = {
                //     // serde_json::from_slice(bytes.as_ref())?
                //     todo!()
                // };
                // let columns = self.get_table(table)?.columns.as_slice();

                // anyhow::ensure!(columns.len() == values.len(), "columns count");

                // Some(ResultRow { columns, values })
            }
            None => None,
        };

        Ok(record)
    }

    pub fn scan<'a, T: serde::de::DeserializeOwned>(
        &'a self,
        table: &str,
    ) -> anyhow::Result<impl Iterator<Item = anyhow::Result<T>> + 'a> {
        let tree = self.db.open_tree(table)?;
        let schema = self.schema()?;
        let table = schema.get_table(table)?;
        let table = table.clone();

        let iterator = tree.iter().map(move |result| {
            result
                .map_err(anyhow::Error::from)
                .and_then(|(key, value)| record_reader::read_record(&key, &value, &table))
        });

        Ok(iterator)
    }
}

fn get_schema(system_table: &sled::Tree) -> Result<Schema, anyhow::Error> {
    let schema = system_table
        .get("schema")?
        .map(|bytes| serde_json::from_slice(bytes.as_ref()).expect("sled schema deserialization failed"))
        .unwrap_or_else(Schema::empty);

    Ok(schema)
}

fn validate_insert(table: &schema::Table, value: &HashMap<&str, DatabaseValue>) -> Result<(), anyhow::Error> {
    for (name, value) in value.iter() {
        let col = table.columns.iter().find(|col| col.name.as_str() == *name);

        match col {
            Some(col) => match (col.r#type, value) {
                (schema::ColumnType::String, DatabaseValue::String(_)) => (),
                (schema::ColumnType::I32, DatabaseValue::I32(_)) => (),
                (schema::ColumnType::F64, DatabaseValue::F64(_)) => (),
                (schema::ColumnType::Boolean, DatabaseValue::Boolean(_)) => (),
                _ => anyhow::bail!("Column type mismatch for {colname}.", colname = col.name),
            },
            None => anyhow::bail!(
                "There is no `{column}` column on `{table}`.",
                column = name,
                table = &table.name
            ),
        }
    }

    Ok(())
}
