use serde::{Deserialize, Serialize};
use sled_wrapper::schema;

type BoxId = u32;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Cat {
    name: String,
    age: usize,
    r#box: Option<BoxId>,
}

fn cat_table() -> schema::Table {
    schema::Table {
        name: "Cat".to_owned(),
        columns: vec![schema::Column {
            name: "age".into(),
            required: true,
            r#type: schema::ColumnType::I32,
        }],
        id_columns: vec![schema::Column {
            name: "name".into(),
            required: true,
            r#type: schema::ColumnType::String,
        }],
    }
}

fn setup_database() -> anyhow::Result<sled_wrapper::Connection> {
    let mut conn = sled_wrapper::Connection::new("testdb".into())?;

    conn.migrate(|schema| schema.tables.push(cat_table()))?;

    Ok(conn)
}

#[test]
fn cats_roundtrip() {
    let db = setup_database().unwrap();

    let felix = Cat {
        name: "Felix".into(),
        age: 20,
        r#box: None,
    };

    db.insert("Cat", &felix).unwrap();

    let roundtripped_felix: Cat = dbg!(db.get("Cat", "Felix")).unwrap().unwrap();

    assert_eq!(felix, roundtripped_felix);
}
