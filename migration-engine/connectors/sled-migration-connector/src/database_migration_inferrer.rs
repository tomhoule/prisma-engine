use crate::SledMigration;
use migration_connector::*;

pub struct SledDatabaseMigrationInferrer;

#[async_trait::async_trait]
impl DatabaseMigrationInferrer<crate::SledMigration> for SledDatabaseMigrationInferrer {
    async fn infer(
        &self,
        datamodel: &datamodel::dml::Datamodel,
        datamodel_2: &datamodel::dml::Datamodel,
        steps: &[MigrationStep],
    ) -> ConnectorResult<SledMigration> {
        let previous_schema = calculate_schema(datamodel);
        let next_schema = calculate_schema(datamodel_2);
        Ok(SledMigration {
            steps: dbg!(diff(&previous_schema, &next_schema)),
        })
    }

    async fn infer_from_datamodels(
        &self,
        datamodel: &datamodel::dml::Datamodel,
        datamodel_2: &datamodel::dml::Datamodel,
        steps: &[MigrationStep],
    ) -> ConnectorResult<SledMigration> {
        let previous_schema = calculate_schema(datamodel);
        let next_schema = calculate_schema(datamodel_2);
        Ok(SledMigration {
            steps: diff(&previous_schema, &next_schema),
        })
    }
}

fn calculate_schema(datamodel: &datamodel::dml::Datamodel) -> sled_wrapper::schema::Schema {
    use sled_wrapper::schema;
    let mut schema = schema::Schema::empty();

    for model in datamodel.models() {
        schema.tables.push(schema::Table::new(model.name.clone()));
    }

    schema
}

fn diff(previous: &sled_wrapper::schema::Schema, next: &sled_wrapper::schema::Schema) -> Vec<crate::SledMigrationStep> {
    let differ = SledSchemaDiffer { previous, next };
    let mut steps = Vec::new();

    for table in differ.created_tables() {
        steps.push(crate::SledMigrationStep::CreateTable(table.clone()))
    }

    for table in differ.dropped_tables() {
        steps.push(crate::SledMigrationStep::DropTable(table.name.clone()));
    }

    steps
}

struct SledSchemaDiffer<'a> {
    previous: &'a sled_wrapper::schema::Schema,
    next: &'a sled_wrapper::schema::Schema,
}

impl<'a> SledSchemaDiffer<'a> {
    fn created_tables(&self) -> impl Iterator<Item = &sled_wrapper::schema::Table> {
        self.next_tables()
            .filter(move |next| !self.previous_tables().any(|previous| previous.name == next.name))
    }

    fn dropped_tables(&self) -> impl Iterator<Item = &sled_wrapper::schema::Table> {
        self.previous_tables()
            .filter(move |previous| !self.next_tables().any(|next| previous.name == next.name))
    }

    fn previous_tables(&self) -> impl Iterator<Item = &sled_wrapper::schema::Table> {
        self.previous.tables.iter()
    }

    fn next_tables(&self) -> impl Iterator<Item = &sled_wrapper::schema::Table> {
        self.next.tables.iter()
    }
}
