pub mod migrations;
mod record_reader;
mod record_writer;
pub mod schema;

use schema::Schema;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::HashMap;

pub type DateTime = chrono::DateTime<chrono::Utc>;

#[derive(Debug)]
pub struct Connection {
    db: sled::Db,
    system_table: sled::Tree,
    // Should probably be an Arc<Mutex<Schema>>
    schema: Schema,
    db_path: std::path::PathBuf,
}

impl Connection {
    pub fn new(file_path: std::path::PathBuf) -> Result<Self, anyhow::Error> {
        let db = sled::open(&file_path)?;
        let system_table = db.open_tree("_System")?;
        let schema = get_schema(&system_table)?;

        Ok(Connection {
            db,
            system_table,
            schema,
            db_path: file_path,
        })
    }

    pub fn get_schema(&self) -> Result<Schema, anyhow::Error> {
        // TODO: maybe refresh the local schema at this point?
        get_schema(&self.system_table)
    }

    pub fn persist_schema(&self, schema: &Schema) -> Result<(), anyhow::Error> {
        let bytes = serde_json::to_vec(schema)?;
        self.system_table.insert("schema", bytes.as_slice())?;

        Ok(())
    }

    pub fn insert(&self, table_name: &str, value: impl serde::Serialize) -> Result<(), anyhow::Error> {
        let table = self.get_table(table_name)?;
        let key_record = record_writer::RecordWriter {
            columns: &table.id_columns,
            data: &value,
        };
        let key_buf = serde_json::to_vec(&key_record)?;
        let value_record = record_writer::RecordWriter {
            columns: &table.columns,
            data: &value,
        };
        let value_buf = serde_json::to_vec(&value_record)?;

        let tree = self.db.open_tree(table_name)?;
        tree.insert(key_buf, value_buf)?;

        Ok(())
    }

    fn get_table<'a>(&'a self, name: &str) -> Result<&'a schema::Table, anyhow::Error> {
        self.schema
            .tables
            .iter()
            .find(|table| table.name == name)
            .ok_or_else(|| anyhow::anyhow!("Unknown table: `{}`", name))
    }

    pub fn get<'a, T: serde::de::DeserializeOwned>(
        &'a self,
        table: &str,
        id: &[&DatabaseValue<'_>],
    ) -> Result<Option<T>, anyhow::Error> {
        let tree = self.db.open_tree(table)?;
        let table = self.get_table(table)?;
        let id_bytes = serde_json::to_vec(id)?;
        let bytes = tree.get(&id_bytes)?;

        let record = match bytes {
            Some(bytes) => {
                record_reader::read_record(&id_bytes, &bytes, table)?
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
        let table = self.get_table(table)?;

        let iterator = tree.iter().map(move |result| {
            result
                .map_err(anyhow::Error::from)
                .and_then(move |(key, value)| record_reader::read_record(&key, &value, table))
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

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DatabaseValue<'a> {
    #[serde(borrow)]
    String(Cow<'a, str>),
    I32(i32),
    F64(f64), // maybe replace with decimal
    DateTime(DateTime),
    Boolean(bool),
}
