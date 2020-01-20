use migration_connector::*;

pub struct SledDestructiveChangesChecker;

#[async_trait::async_trait]
impl DestructiveChangesChecker<crate::SledMigration> for SledDestructiveChangesChecker {
    async fn check(&self, _: &crate::SledMigration) -> ConnectorResult<DestructiveChangeDiagnostics> {
        Ok(DestructiveChangeDiagnostics::new())
    }

    async fn check_unapply(&self, _: &crate::SledMigration) -> ConnectorResult<DestructiveChangeDiagnostics> {
        Ok(DestructiveChangeDiagnostics::new())
    }
}
