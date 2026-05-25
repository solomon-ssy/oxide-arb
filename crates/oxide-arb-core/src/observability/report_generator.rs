use oxide_arb_error::OxideError;

pub struct ReportGenerator {}

impl ReportGenerator {
    pub const fn new() -> Self {
        Self {}
    }

    pub fn generate_daily(&self) -> Result<(), OxideError> {
        tracing::info!("daily report generation — not yet implemented");
        Ok(())
    }

    pub fn generate_weekly(&self) -> Result<(), OxideError> {
        tracing::info!("weekly report generation — not yet implemented");
        Ok(())
    }
}

impl Default for ReportGenerator {
    fn default() -> Self {
        Self::new()
    }
}
