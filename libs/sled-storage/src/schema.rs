use std::collections::BTreeMap;

pub(crate) type ColumnId = usize;

#[derive(Debug)]
pub(crate) struct TableMetadata {
    pub(crate) name: String,
    pub(crate) columns: BTreeMap<ColumnId, ColumnMetadata>,
    pub(crate) column_names: BTreeMap<String, ColumnMetadata>,
}

impl TableMetadata {
    pub(crate) fn from_name_and_columns(name: String, columns: &[ColumnMetadata]) -> TableMetadata {
        TableMetadata {
            name,
            columns: columns.iter().map(|col| (col.column_id, col.to_owned())).collect(),
            column_names: columns.iter().map(|col| (col.name.clone(), col.to_owned())).collect(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ColumnMetadata {
    /// The index of the column in the table data.
    pub(crate) column_id: ColumnId,
    pub(crate) name: String,
    pub(crate) r#type: ColumnType,
}

#[derive(Debug, Clone)]
pub(crate) enum ColumnType {
    Text,
    I32,
    Decimal,
}
