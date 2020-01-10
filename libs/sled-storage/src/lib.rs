mod schema;

use std::collections::BTreeMap;

fn tables_table_schema() -> schema::TableMetadata {
    let columns = &[schema::ColumnMetadata {
        column_id: 1,
        name: "name".to_owned(),
        r#type: schema::ColumnType::Text,
    }];
    schema::TableMetadata::from_name_and_columns("_Table".to_owned(), columns)
}

fn columns_table_schema() -> schema::TableMetadata {
    todo!()
}

#[derive(Debug)]
pub struct Database {
    system_tables: SystemTables,
    tables: std::collections::BTreeMap<String, Table>,
    db: sled::Db,
}

impl Database {
    pub fn new(db: sled::Db) -> Result<Self, sled::Error> {
        let tables_table = tables_table_schema();
        let columns_table = columns_table_schema();
        let system_tables = SystemTables {
            columns: Table {
                tree: db.open_tree(columns_table.name.as_bytes())?,
                schema: columns_table,
            },
            tables: Table {
                tree: db.open_tree(tables_table.name.as_bytes())?,
                schema: tables_table,
            },
        };

        Ok(Database {
            db,
            system_tables,
            tables: BTreeMap::new(),
        })
    }
}

#[derive(Debug)]
struct SystemTables {
    tables: Table,
    columns: Table,
}

#[derive(Debug)]
pub struct Table {
    schema: schema::TableMetadata,
    tree: sled::Tree,
}

impl Table {
    pub fn get<'a>(&'a self, id: impl AsRef<[u8]>) -> Option<ResultRow<'a>> {
        self.tree.get(id).ok().and_then(|row| {
            row.map(|row| ResultRow {
                schema: &self.schema,
                row,
            })
        })
    }

    pub fn insert(&self, data: impl serde::Serialize) {
        todo!()
    }
}

impl serde::Serializer for schema::TableMetadata {
    type Ok = ();
    type Error = serde::de::value::Error;
    type SerializeSeq = serde::ser::Impossible<Self::Ok, Self::Error>;
    type SerializeTuple = serde::ser::Impossible<Self::Ok, Self::Error>;
    type SerializeTupleStruct = serde::ser::Impossible<Self::Ok, Self::Error>;
    type SerializeTupleVariant = serde::ser::Impossible<Self::Ok, Self::Error>;
    type SerializeMap = serde::ser::Impossible<Self::Ok, Self::Error>;
    type SerializeStruct = serde::ser::Impossible<Self::Ok, Self::Error>;
    type SerializeStructVariant = serde::ser::Impossible<Self::Ok, Self::Error>;

    fn serialize_bool(self, b: bool) -> Result<Self::Ok, Self::Error> {
        todo!()
    }
}

pub struct ResultRow<'a> {
    schema: &'a schema::TableMetadata,
    row: sled::IVec,
}

pub struct RowBuilder<'a> {
    key: &'a mut Vec<u8>,
    value: &'a mut Vec<u8>,
    tree: &'a sled::Tree,
}

impl<'a> RowBuilder<'a> {
    fn build_by_names(&mut self, values: &[(&str, &Value)]) {}
}

enum Value {
    Text(String),
    I32(i32),
    Float(String, String),
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
