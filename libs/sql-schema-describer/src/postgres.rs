//! Postgres description.
use super::*;
use log::debug;
use once_cell::sync::Lazy;
use quaint::prelude::Queryable;
use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::sync::Arc;

pub struct SqlSchemaDescriber {
    conn: Arc<dyn Queryable + Send + Sync + 'static>,
}

#[async_trait::async_trait]
impl super::SqlSchemaDescriberBackend for SqlSchemaDescriber {
    async fn list_databases(&self) -> SqlSchemaDescriberResult<Vec<String>> {
        let databases = self.get_databases().await;
        Ok(databases)
    }

    async fn get_metadata(&self, schema: &str) -> SqlSchemaDescriberResult<SQLMetadata> {
        let count = self.get_table_names(&schema).await.len();
        let size = self.get_size(&schema).await;
        Ok(SQLMetadata {
            table_count: count,
            size_in_bytes: size,
        })
    }

    async fn describe(&self, schema: &str) -> SqlSchemaDescriberResult<SqlSchema> {
        debug!("describing schema '{}'", schema);
        let sequences = self.get_sequences(schema).await?;
        let table_names = self.get_table_names(schema).await;

        let mut tables = Vec::with_capacity(table_names.len());

        for table_name in &table_names {
            tables.push(self.get_table(schema, &table_name, &sequences).await);
        }

        let enums = self.get_enums(schema).await?;
        Ok(SqlSchema {
            enums,
            sequences,
            tables,
        })
    }
}

impl SqlSchemaDescriber {
    /// Constructor.
    pub fn new(conn: Arc<dyn Queryable + Send + Sync + 'static>) -> SqlSchemaDescriber {
        SqlSchemaDescriber { conn }
    }

    async fn get_databases(&self) -> Vec<String> {
        debug!("Getting databases");
        let sql = "select schema_name from information_schema.schemata;";
        let rows = self.conn.query_raw(sql, &[]).await.expect("get schema names ");
        let names = rows
            .into_iter()
            .map(|row| {
                row.get("schema_name")
                    .and_then(|x| x.to_string())
                    .expect("convert schema names")
            })
            .collect();

        debug!("Found schema names: {:?}", names);
        names
    }

    async fn get_table_names(&self, schema: &str) -> Vec<String> {
        debug!("Getting table names");
        let sql = "SELECT table_name as table_name FROM information_schema.tables
            WHERE table_schema = $1
            -- Views are not supported yet
            AND table_type = 'BASE TABLE'
            ORDER BY table_name";
        let rows = self
            .conn
            .query_raw(sql, &[schema.into()])
            .await
            .expect("get table names ");
        let names = rows
            .into_iter()
            .map(|row| {
                row.get("table_name")
                    .and_then(|x| x.to_string())
                    .expect("get table name")
            })
            .collect();

        debug!("Found table names: {:?}", names);
        names
    }

    async fn get_size(&self, schema: &str) -> usize {
        debug!("Getting db size");
        let sql =
            "SELECT SUM(pg_total_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename)))::BIGINT as size
             FROM pg_tables
             WHERE schemaname = $1::text";
        let result = self.conn.query_raw(sql, &[schema.into()]).await.expect("get db size ");
        let size: i64 = result
            .first()
            .map(|row| row.get("size").and_then(|x| x.as_i64()).unwrap_or(0))
            .unwrap();

        debug!("Found db size: {:?}", size);
        size.try_into().unwrap()
    }

    async fn get_table(&self, schema: &str, name: &str, sequences: &Vec<Sequence>) -> Table {
        debug!("Getting table '{}'", name);
        let columns = self.get_columns(schema, name).await;
        let (indices, primary_key) = self.get_indices(schema, name, sequences).await;
        let foreign_keys = self.get_foreign_keys(schema, name).await;
        Table {
            name: name.to_string(),
            columns,
            foreign_keys,
            indices,
            primary_key,
        }
    }

    async fn get_columns(&self, schema: &str, table: &str) -> Vec<Column> {
        let sql = "SELECT column_name, data_type, udt_name as full_column_type, column_default, is_nullable, is_identity, data_type
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
            ORDER BY column_name";
        let rows = self
            .conn
            .query_raw(&sql, &[schema.into(), table.into()])
            .await
            .expect("querying for columns");
        let cols = rows
            .into_iter()
            .map(|col| {
                debug!("Got column: {:?}", col);
                let col_name = col
                    .get("column_name")
                    .and_then(|x| x.to_string())
                    .expect("get column name");
                let data_type = col.get("data_type").and_then(|x| x.to_string()).expect("get data_type");
                let full_data_type = col
                    .get("full_column_type")
                    .and_then(|x| x.to_string())
                    .expect("get full_column_type aka udt_name");
                let is_identity_str = col
                    .get("is_identity")
                    .and_then(|x| x.to_string())
                    .expect("get is_identity")
                    .to_lowercase();
                let is_identity = match is_identity_str.as_str() {
                    "no" => false,
                    "yes" => true,
                    _ => panic!("unrecognized is_identity variant '{}'", is_identity_str),
                };
                let is_nullable = col
                    .get("is_nullable")
                    .and_then(|x| x.to_string())
                    .expect("get is_nullable")
                    .to_lowercase();
                let is_required = match is_nullable.as_ref() {
                    "no" => true,
                    "yes" => false,
                    x => panic!(format!("unrecognized is_nullable variant '{}'", x)),
                };

                let arity = if full_data_type.starts_with("_") {
                    ColumnArity::List
                } else if is_required {
                    ColumnArity::Required
                } else {
                    ColumnArity::Nullable
                };
                let tpe = get_column_type(data_type.as_ref(), &full_data_type, arity);

                let default = col.get("column_default").and_then(|param_value| {
                    param_value
                        .to_string()
                        .map(|x| x.replace("\'", "").replace("::text", ""))
                });
                let is_auto_increment = is_identity
                    || match default {
                        Some(ref val) => is_autoincrement(val, schema, table, &col_name),
                        _ => false,
                    };
                Column {
                    name: col_name,
                    tpe,
                    default,
                    auto_increment: is_auto_increment,
                }
            })
            .collect();

        debug!("Found table columns: {:?}", cols);
        cols
    }

    async fn get_foreign_keys(&self, schema: &str, table: &str) -> Vec<ForeignKey> {
        // The `generate_subscripts` in the inner select is needed because the optimizer is free to reorganize the unnested rows if not explicitly ordered.
        let sql = r#"
            SELECT
                con.oid as "con_id",
                att2.attname as "child_column",
                cl.relname as "parent_table",
                att.attname as "parent_column",
                con.confdeltype,
                conname as constraint_name,
                child,
                parent
            FROM
            (SELECT
                    unnest(con1.conkey) as "parent",
                    unnest(con1.confkey) as "child",
                    generate_subscripts(con1.conkey, 1) AS colidx,
                    con1.oid,
                    con1.confrelid,
                    con1.conrelid,
                    con1.conname,
                    con1.confdeltype
                FROM
                    pg_class cl
                    join pg_namespace ns on cl.relnamespace = ns.oid
                    join pg_constraint con1 on con1.conrelid = cl.oid
                WHERE
                    cl.relname = $1
                    and ns.nspname = $2
                    and con1.contype = 'f'
                    ORDER BY colidx
            ) con
            JOIN pg_attribute att on
                att.attrelid = con.confrelid and att.attnum = con.child
            JOIN pg_class cl on
                cl.oid = con.confrelid
            JOIN pg_attribute att2 on
                att2.attrelid = con.conrelid and att2.attnum = con.parent
            ORDER BY con_id, con.colidx"#;
        debug!("describing table foreign keys, SQL: '{}'", sql);

        // One foreign key with multiple columns will be represented here as several
        // rows with the same ID, which we will have to combine into corresponding foreign key
        // objects.
        let result_set = self
            .conn
            .query_raw(&sql, &[table.into(), schema.into()])
            .await
            .expect("querying for foreign keys");
        let mut intermediate_fks: HashMap<i64, ForeignKey> = HashMap::new();
        for row in result_set.into_iter() {
            debug!("Got description FK row {:?}", row);
            let id = row.get("con_id").and_then(|x| x.as_i64()).expect("get con_id");
            let column = row
                .get("child_column")
                .and_then(|x| x.to_string())
                .expect("get child_column");
            let referenced_table = row
                .get("parent_table")
                .and_then(|x| x.to_string())
                .expect("get parent_table");
            let referenced_column = row
                .get("parent_column")
                .and_then(|x| x.to_string())
                .expect("get parent_column");
            let confdeltype = row
                .get("confdeltype")
                .and_then(|x| x.as_char())
                .expect("get confdeltype");
            let constraint_name = row
                .get("constraint_name")
                .and_then(|x| x.to_string())
                .expect("get constraint_name");
            let on_delete_action = match confdeltype {
                'a' => ForeignKeyAction::NoAction,
                'r' => ForeignKeyAction::Restrict,
                'c' => ForeignKeyAction::Cascade,
                'n' => ForeignKeyAction::SetNull,
                'd' => ForeignKeyAction::SetDefault,
                _ => panic!(format!("unrecognized foreign key action '{}'", confdeltype)),
            };
            match intermediate_fks.get_mut(&id) {
                Some(fk) => {
                    fk.columns.push(column);
                    fk.referenced_columns.push(referenced_column);
                }
                None => {
                    let fk = ForeignKey {
                        constraint_name: Some(constraint_name),
                        columns: vec![column],
                        referenced_table,
                        referenced_columns: vec![referenced_column],
                        on_delete_action,
                    };
                    intermediate_fks.insert(id, fk);
                }
            };
        }

        let mut fks: Vec<ForeignKey> = intermediate_fks
            .values()
            .map(|intermediate_fk| intermediate_fk.to_owned())
            .collect();
        for fk in fks.iter() {
            debug!(
                "Found foreign key - column(s): {:?}, to table: '{}', to column(s): {:?}",
                fk.columns, fk.referenced_table, fk.referenced_columns
            );
        }

        fks.sort_unstable_by_key(|fk| fk.columns.clone());

        fks
    }

    async fn get_indices(
        &self,
        schema: &str,
        table_name: &str,
        sequences: &Vec<Sequence>,
    ) -> (Vec<Index>, Option<PrimaryKey>) {
        let sql = r#"
        SELECT
            indexInfos.relname as name,
            array_agg(columnInfos.attname) as column_names,
            rawIndex.indisunique as is_unique, rawIndex.indisprimary as is_primary_key
        FROM
            -- pg_class stores infos about tables, indices etc: https://www.postgresql.org/docs/current/catalog-pg-class.html
            pg_class tableInfos,
            pg_class indexInfos,
            -- pg_index stores indices: https://www.postgresql.org/docs/current/catalog-pg-index.html
            (
                SELECT
                    indrelid,
                    indexrelid,
                    indisunique,
                    indisprimary,
                    unnest(array_agg(pg_index.indkey)) AS indkey,
                    generate_subscripts(array_agg(pg_index.indkey), 1) AS indkeyidx
                FROM pg_index
                GROUP BY indrelid, indexrelid, indisunique, indisprimary
                ORDER BY indkeyidx
            ) rawIndex,
            -- pg_attribute stores infos about columns: https://www.postgresql.org/docs/current/catalog-pg-attribute.html
            pg_attribute columnInfos,
            -- pg_namespace stores info about the schema
            pg_namespace schemaInfo
        WHERE
            -- find table info for index
            tableInfos.oid = rawIndex.indrelid
            -- find index info
            AND indexInfos.oid = rawIndex.indexrelid
            -- find table columns
            AND columnInfos.attrelid = tableInfos.oid
            AND columnInfos.attnum = rawIndex.indkey
            -- we only consider ordinary tables
            AND tableInfos.relkind = 'r'
            -- we only consider stuff out of one specific schema
            AND tableInfos.relnamespace = schemaInfo.oid
            AND schemaInfo.nspname = $1
            AND tableInfos.relname = $2
        GROUP BY tableInfos.relname, indexInfos.relname, rawIndex.indisunique, rawIndex.indisprimary
        "#;
        debug!("Getting indices: {}", sql);
        let rows = self
            .conn
            .query_raw(&sql, &[schema.into(), table_name.into()])
            .await
            .expect("querying for indices");
        let mut pk: Option<PrimaryKey> = None;

        let mut indices = Vec::new();

        for index in rows {
            debug!("Got index: {:?}", index);
            let is_pk = index
                .get("is_primary_key")
                .and_then(|x| x.as_bool())
                .expect("get is_primary_key");
            // TODO: Implement and use as_slice instead of into_vec, to avoid cloning
            let columns = index
                .get("column_names")
                .and_then(|x| x.clone().into_vec::<String>())
                .expect("column_names");
            if is_pk {
                pk = Some(self.infer_primary_key(schema, table_name, columns, sequences).await);
            } else {
                let is_unique = index.get("is_unique").and_then(|x| x.as_bool()).expect("is_unique");
                indices.push(Index {
                    name: index.get("name").and_then(|x| x.to_string()).expect("name"),
                    columns,
                    tpe: match is_unique {
                        true => IndexType::Unique,
                        false => IndexType::Normal,
                    },
                })
            }
        }

        debug!("Found table indices: {:?}, primary key: {:?}", indices, pk);
        (indices, pk)
    }

    async fn infer_primary_key(
        &self,
        schema: &str,
        table_name: &str,
        columns: Vec<String>,
        sequences: &Vec<Sequence>,
    ) -> PrimaryKey {
        let sequence = if columns.len() == 1 {
            let sql = format!(
                "SELECT pg_get_serial_sequence('\"{}\".\"{}\"', '{}') as sequence",
                schema, table_name, columns[0]
            );
            debug!(
                "Querying for sequence seeding primary key column '{}': '{}'",
                columns[0], sql
            );
            let rows = self
                .conn
                .query_raw(&sql, &[])
                .await
                .expect("querying for sequence seeding primary key column");
            // Given the result rows, find any sequence
            rows.into_iter().fold(None, |_: Option<Sequence>, row| {
                row.get("sequence")
                    .and_then(|x| x.to_string())
                    .and_then(|sequence_name| {
                        let captures = RE_SEQ.captures(&sequence_name).expect("get captures");
                        let sequence_name = captures.get(1).expect("get capture").as_str();
                        debug!("Found sequence name corresponding to primary key: {}", sequence_name);
                        sequences.iter().find(|s| &s.name == sequence_name).map(|sequence| {
                            debug!("Got sequence corresponding to primary key: {:#?}", sequence);
                            sequence.clone()
                        })
                    })
            })
        } else {
            None
        };

        PrimaryKey { columns, sequence }
    }

    async fn get_sequences(&self, schema: &str) -> SqlSchemaDescriberResult<Vec<Sequence>> {
        debug!("Getting sequences");
        let sql = "SELECT start_value, sequence_name
                  FROM information_schema.sequences
                  WHERE sequence_schema = $1";
        let rows = self
            .conn
            .query_raw(&sql, &[schema.into()])
            .await
            .expect("querying for sequences");
        let sequences = rows
            .into_iter()
            .map(|seq| {
                debug!("Got sequence: {:?}", seq);
                let initial_value = seq
                    .get("start_value")
                    .and_then(|x| x.to_string())
                    .and_then(|x| x.parse::<u32>().ok())
                    .expect("get start_value");
                Sequence {
                    // Not sure what allocation size refers to, but the TypeScript implementation
                    // hardcodes this as 1
                    allocation_size: 1,
                    initial_value,
                    name: seq
                        .get("sequence_name")
                        .and_then(|x| x.to_string())
                        .expect("get sequence_name"),
                }
            })
            .collect();

        debug!("Found sequences: {:?}", sequences);
        Ok(sequences)
    }

    async fn get_enums(&self, schema: &str) -> SqlSchemaDescriberResult<Vec<Enum>> {
        debug!("Getting enums");
        let sql = "SELECT t.typname as name, e.enumlabel as value
            FROM pg_type t
            JOIN pg_enum e ON t.oid = e.enumtypid
            JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
            WHERE n.nspname = $1";
        let rows = self
            .conn
            .query_raw(&sql, &[schema.into()])
            .await
            .expect("querying for enums");
        let mut enum_values: HashMap<String, HashSet<String>> = HashMap::new();
        for row in rows.into_iter() {
            debug!("Got enum row: {:?}", row);
            let name = row.get("name").and_then(|x| x.to_string()).expect("get name");
            let value = row.get("value").and_then(|x| x.to_string()).expect("get value");
            if !enum_values.contains_key(&name) {
                enum_values.insert(name.clone(), HashSet::new());
            }
            let vals = enum_values.get_mut(&name).expect("get enum values");
            vals.insert(value);
        }

        let enums: Vec<Enum> = enum_values
            .into_iter()
            .map(|(k, v)| Enum { name: k, values: v })
            .collect();
        debug!("Found enums: {:?}", enums);
        Ok(enums)
    }
}

fn get_column_type(_data_type: &str, full_data_type: &str, arity: ColumnArity) -> ColumnType {
    let family = match full_data_type {
        "int2" => ColumnTypeFamily::Int,
        "int4" => ColumnTypeFamily::Int,
        "int8" => ColumnTypeFamily::Int,
        "float4" => ColumnTypeFamily::Float,
        "float8" => ColumnTypeFamily::Float,
        "bool" => ColumnTypeFamily::Boolean,
        "text" => ColumnTypeFamily::String,
        "varchar" => ColumnTypeFamily::String,
        "date" => ColumnTypeFamily::DateTime,
        "bytea" => ColumnTypeFamily::Binary,
        "json" => ColumnTypeFamily::Json,
        "jsonb" => ColumnTypeFamily::Json,
        "uuid" => ColumnTypeFamily::Uuid,
        "bit" => ColumnTypeFamily::Binary,
        "varbit" => ColumnTypeFamily::Binary,
        "box" => ColumnTypeFamily::Geometric,
        "circle" => ColumnTypeFamily::Geometric,
        "line" => ColumnTypeFamily::Geometric,
        "lseg" => ColumnTypeFamily::Geometric,
        "path" => ColumnTypeFamily::Geometric,
        "polygon" => ColumnTypeFamily::Geometric,
        "bpchar" => ColumnTypeFamily::String,
        "interval" => ColumnTypeFamily::DateTime,
        "numeric" => ColumnTypeFamily::Float,
        "pg_lsn" => ColumnTypeFamily::LogSequenceNumber,
        "time" => ColumnTypeFamily::DateTime,
        "timetz" => ColumnTypeFamily::DateTime,
        "timestamp" => ColumnTypeFamily::DateTime,
        "timestamptz" => ColumnTypeFamily::DateTime,
        "tsquery" => ColumnTypeFamily::TextSearch,
        "tsvector" => ColumnTypeFamily::TextSearch,
        "txid_snapshot" => ColumnTypeFamily::TransactionId,
        // Array types
        "_bytea" => ColumnTypeFamily::Binary,
        "_bool" => ColumnTypeFamily::Boolean,
        "_date" => ColumnTypeFamily::DateTime,
        "_float8" => ColumnTypeFamily::Float,
        "_float4" => ColumnTypeFamily::Float,
        "_int4" => ColumnTypeFamily::Int,
        "_text" => ColumnTypeFamily::String,
        "_varchar" => ColumnTypeFamily::String,
        _ => ColumnTypeFamily::Unknown,
    };
    ColumnType {
        raw: full_data_type.to_string(),
        family: family,
        arity,
    }
}

static RE_SEQ: Lazy<Regex> = Lazy::new(|| Regex::new("^(?:.+\\.)?\"?([^.\"]+)\"?").expect("compile regex"));

static AUTOINCREMENT_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"nextval\((?:"(?P<schema_name>.+)"\.)?"(?P<table_and_column_name>.+)_seq(?:[0-9]+)?"::regclass\)"#)
        .unwrap()
});

/// Returns whether a particular sequence (`value`) matches the provided column info.
fn is_autoincrement(value: &str, schema_name: &str, table_name: &str, column_name: &str) -> bool {
    AUTOINCREMENT_REGEX
        .captures(value)
        .and_then(|captures| {
            captures
                .name("schema_name")
                .map(|matched| matched.as_str())
                .or(Some(schema_name))
                .filter(|matched| *matched == schema_name)
                .and_then(|_| {
                    captures.name("table_and_column_name").filter(|matched| {
                        let expected_len = table_name.len() + column_name.len() + 1;

                        if matched.as_str().len() != expected_len {
                            return false;
                        }

                        let table_name_segments = table_name.split('_');
                        let column_name_segments = column_name.split('_');
                        let matched_segments = matched.as_str().split('_');
                        matched_segments
                            .zip(table_name_segments.chain(column_name_segments))
                            .all(|(found, expected)| found == expected)
                    })
                })
                .map(|_| true)
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_is_autoincrement_works() {
        let schema_name = "prisma";
        let table_name = "Test";
        let col_name = "id";

        let non_autoincrement = "_seq";
        assert!(!is_autoincrement(non_autoincrement, schema_name, table_name, col_name));

        let autoincrement = format!(
            r#"nextval("{}"."{}_{}_seq"::regclass)"#,
            schema_name, table_name, col_name
        );
        assert!(is_autoincrement(&autoincrement, schema_name, table_name, col_name));

        let autoincrement_with_number = format!(
            r#"nextval("{}"."{}_{}_seq1"::regclass)"#,
            schema_name, table_name, col_name
        );
        assert!(is_autoincrement(
            &autoincrement_with_number,
            schema_name,
            table_name,
            col_name
        ));

        let autoincrement_without_schema = format!(r#"nextval("{}_{}_seq1"::regclass)"#, table_name, col_name);
        assert!(is_autoincrement(
            &autoincrement_without_schema,
            schema_name,
            table_name,
            col_name
        ));

        // The table and column names contain underscores, so it's impossible to say from the sequence where one starts and the other ends.
        let autoincrement_with_ambiguous_table_and_column_names =
            r#"nextval("compound_table_compound_column_name_seq"::regclass)"#;
        assert!(is_autoincrement(
            &autoincrement_with_ambiguous_table_and_column_names,
            "<ignored>",
            "compound_table",
            "compound_column_name",
        ));

        // The table and column names contain underscores, so it's impossible to say from the sequence where one starts and the other ends.
        // But this one has extra text between table and column names, so it should not match.
        let autoincrement_with_ambiguous_table_and_column_names =
            r#"nextval("compound_table_something_compound_column_name_seq"::regclass)"#;
        assert!(!is_autoincrement(
            &autoincrement_with_ambiguous_table_and_column_names,
            "<ignored>",
            "compound_table",
            "compound_column_name",
        ));
    }
}
