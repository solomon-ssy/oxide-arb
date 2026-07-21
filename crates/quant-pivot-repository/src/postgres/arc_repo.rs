//! Helpers for wiring `Arc<Pg*Repository>` from a shared [`DatabaseConnection`].

use std::sync::Arc;

use sea_orm::DatabaseConnection;

/// Construct an `Arc`-wrapped Postgres repository from a shared connection clone.
///
/// Every `Pg*Repository::new` takes ownership of a [`DatabaseConnection`]; boot
/// wiring shares one handle and clones per repository.
pub fn arc_repo<R, F>(db: &DatabaseConnection, new: F) -> Arc<R>
where
    F: FnOnce(DatabaseConnection) -> R,
{
    Arc::new(new(db.clone()))
}

/// Sugar for [`arc_repo`] when the constructor is `RepositoryType::new`.
#[macro_export]
macro_rules! pg_arc_repo {
    ($db:expr, $ty:ty) => {
        $crate::postgres::arc_repo(&$db, <$ty>::new)
    };
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use sea_orm::DatabaseConnection;

    use super::arc_repo;

    /// Lightweight stand-in proving [`arc_repo`] clones the connection handle.
    struct DummyRepo {
        _connection: DatabaseConnection,
    }

    impl DummyRepo {
        fn new(db: DatabaseConnection) -> Self {
            Self { _connection: db }
        }
    }

    #[test]
    fn arc_repo_produces_distinct_arc_allocations() {
        let db = DatabaseConnection::default();
        let first: Arc<DummyRepo> = arc_repo(&db, DummyRepo::new);
        let second: Arc<DummyRepo> = arc_repo(&db, DummyRepo::new);

        assert_ne!(Arc::as_ptr(&first), Arc::as_ptr(&second));
    }
}
