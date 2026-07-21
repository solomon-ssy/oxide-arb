//! Permission catalog API contract.

use serde::Serialize;

use crate::enums::rbac::{Operation, ResourceType};

/// One resource and the operations that may be granted on it.
#[derive(Debug, Serialize)]
pub struct PermissionCatalogEntry {
    /// The resource category.
    pub resource: ResourceType,
    /// The operations valid for this resource.
    pub operations: Vec<Operation>,
}
