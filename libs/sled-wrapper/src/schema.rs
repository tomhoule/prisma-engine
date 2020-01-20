use serde::*;

#[derive(Debug, Serialize, Deserialize)]
pub struct Schema {
    pub tables: Vec<Table>,
}

impl Schema {
    pub fn empty() -> Self {
        Schema { tables: Vec::new() }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

impl Table {
    pub fn new(name: impl Into<String>) -> Self {
        Table {
            name: name.into(),
            columns: Vec::new(),
        }
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
