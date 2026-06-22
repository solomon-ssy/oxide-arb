//! Legacy Endgame trading enums — retained only for unmigrated Postgres rows (Phase 1 removal).

active_string_enum! {
    /// Legacy trade execution mode stored on historical `trade` rows.
    @derive(Default)
    pub enum LegacyExecutionMode {
        #[default]
        DryRun => "dry_run",
        Paper => "paper",
        Live => "live",
    }
}
