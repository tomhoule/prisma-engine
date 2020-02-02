use serde::*;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Schema {
    pub tables: Vec<Table>,
}

impl Schema {
    pub fn empty() -> Self {
        Schema { tables: Vec::new() }
    }

    pub(crate) fn get_table<'a>(&'a self, name: &str) -> Result<&'a Table, anyhow::Error> {
        self.tables
            .iter()
            .find(|table| table.name == name)
            .ok_or_else(|| anyhow::anyhow!("Unknown table: `{}`", name))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub id_columns: Vec<Column>,
    pub columns: Vec<Column>,
}

impl Table {
    pub fn new(name: impl Into<String>) -> Self {
        Table {
            name: name.into(),
            id_columns: Vec::new(),
            columns: Vec::new(),
        }
    }

    pub fn find_column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|col| col.name == name)
    }
}

#[derive(Debug, Serialize, Clone, Deserialize)]
pub struct Column {
    pub name: String,
    pub r#type: ColumnType,
    pub required: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum ColumnType {
    String,
    I32,
    F64,
    Boolean,
}
