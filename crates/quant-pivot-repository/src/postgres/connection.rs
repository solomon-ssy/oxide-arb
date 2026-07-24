use sea_orm::{ConnectionTrait, DatabaseConnection, DatabaseTransaction};

/// Connection ownership boundary shared by repositories that can run against
/// either the pool handle or an existing transaction.
pub trait RepositoryConnection: Send + Sync {
    type Connection: ConnectionTrait;

    fn connection(&self) -> &Self::Connection;
}

impl RepositoryConnection for DatabaseConnection {
    type Connection = Self;

    fn connection(&self) -> &Self::Connection {
        self
    }
}

impl RepositoryConnection for &DatabaseTransaction {
    type Connection = DatabaseTransaction;

    fn connection(&self) -> &Self::Connection {
        self
    }
}
