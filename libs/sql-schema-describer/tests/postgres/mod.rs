use log::debug;
use quaint::{prelude::*};
use sql_schema_describer::*;
use std::sync::Arc;

use super::SCHEMA;

pub async fn get_postgres_describer(sql: &str, db_name: &str) -> postgres::SqlSchemaDescriber {
    let host = match std::env::var("IS_BUILDKITE") {
        Ok(_) => "test-db-postgres-10",
        Err(_) => "127.0.0.1",
    };

    let url = format!("postgres://postgres:prisma@{}:5432/{}", host, db_name);
    let client = test_setup::create_postgres_database(&url.parse().unwrap()).await.unwrap();

    let drop_schema = format!("DROP SCHEMA IF EXISTS \"{}\" CASCADE;", SCHEMA);
    client
        .execute_raw(drop_schema.as_str(), &[])
        .await
        .expect("dropping schema");

    debug!("Creating Postgres schema '{}'", SCHEMA);
    client
        .execute_raw(format!("CREATE SCHEMA \"{}\";", SCHEMA).as_str(), &[])
        .await
        .expect("creating schema");

    let sql_string = sql.to_string();
    let statements: Vec<&str> = sql_string.split(";").filter(|s| !s.is_empty()).collect();
    for statement in statements {
        debug!("Executing migration statement: '{}'", statement);
        client
            .execute_raw(statement, &[])
            .await
            .expect("executing migration statement");
    }

    postgres::SqlSchemaDescriber::new(Arc::new(client))
}
