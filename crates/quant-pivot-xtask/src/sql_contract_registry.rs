//! Native-SQL registry owned by repository maintenance tooling.

use quant_pivot_sql_contract::SqlContract;

const XTASK_SQL_CONTRACTS: &[SqlContract] = &[];

/// Return the compiled maintenance-tool native-SQL registry.
#[must_use]
pub const fn xtask_sql_contracts() -> &'static [SqlContract] {
    XTASK_SQL_CONTRACTS
}

#[cfg(test)]
mod tests {
    use quant_pivot_sql_contract::validate_registry;

    use super::XTASK_SQL_CONTRACTS;

    #[test]
    fn xtask_registry_is_valid() {
        assert!(validate_registry(XTASK_SQL_CONTRACTS).is_ok());
    }
}
