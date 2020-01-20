use super::schema::{Column, Schema, Table};
use serde::{Deserialize, Serialize};

pub fn apply(schema: &mut Schema, step: &SledMigration) {
    match step {
        SledMigration::CreateTable(table) => schema.tables.push(table.clone()),
        SledMigration::DropTable(name) => {
            schema.tables = schema.tables.drain(..).filter(|t| t.name.as_str() != name).collect();
        }
        other => todo!("apply {:?}", other),
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum SledMigration {
    CreateTable(Table),
    DropTable(String),
    AlterTable(SledAlterTable),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum SledAlterTable {
    AddColumns(Vec<Column>),
    DropColumns(Vec<String>),
}
