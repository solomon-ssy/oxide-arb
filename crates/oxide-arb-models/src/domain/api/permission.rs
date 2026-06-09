//! Permission catalog API contract.

use crate::enums::rbac::{Operation, ResourceType};
use serde::Serialize;

/// One resource and the operations that may be granted on it.
#[derive(Debug, Serialize)]
pub struct PermissionCatalogEntry {
    /// The resource category.
    pub resource: ResourceType,
    /// The operations valid for this resource.
    pub operations: Vec<Operation>,
}
