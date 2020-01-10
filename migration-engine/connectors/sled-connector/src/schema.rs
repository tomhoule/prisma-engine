use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

type OnDiskRow<'a> = &'a [Option<&'a [u8]>];
type ColumnId = usize;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct TableMetadata {
    pub(crate) name: String,
    pub(crate) columns: BTreeMap<ColumnId, ColumnMetadata>,
    pub(crate) column_names: BTreeMap<String, ColumnMetadata>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct ColumnMetadata {
    /// The index of the column in the table data.
    pub(crate) column_id: ColumnId,
    pub(crate) name: String,
    pub(crate) r#type: String,
}

#[derive(Debug)]
enum ColumnType {
    Text,
    I32,
    Decimal,
}

pub(crate) struct Schema {
    pub(crate) tables: Vec<TableMetadata>,
}
