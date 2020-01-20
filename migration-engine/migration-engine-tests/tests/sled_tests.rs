use migration_engine_tests::{sled::*};
use test_macros::test_dis;

#[test_dis]
async fn sled_create_table(api: TestApi) -> TestResult {
    let dm = r#"
        model Human {
            id String @id
            name String
            cat Cat?
        }

        model Cat {
            id Int @id
            name String
            humans Human[]
        }
    "#;

    api.infer_apply(dm).send_assert().await?.assert_green()?;

    api.assert_schema()?
        .assert_has_table("Human")?
        .assert_has_table("Cat")?;

    Ok(())
}

#[test_dis]
async fn sled_drop_table(api: TestApi) -> TestResult {
    let dm = r#"
        model Human {
            id String @id
            name String
            cat Cat?
        }

        model Cat {
            id Int @id
            name String
            humans Human[]
        }
    "#;

    api.infer_apply(dm).send_assert().await?.assert_green()?;

    api.assert_schema()?
        .assert_has_table("Human")?
        .assert_has_table("Cat")?;

    let dm2 = r#"
        model Cat {
            id Int @id
            name String
        }
    "#;

    api.infer_apply(dm2).send_assert().await?.assert_green()?;

    api.assert_schema()?.assert_does_not_have_table("Human")?;

    Ok(())
}
