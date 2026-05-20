use std::time::Duration;

use crate::config::{Config, TeamFilterMode, TeamList};

/// Shared state handed to every worker loop. With the transactional producer
/// design the producer is owned by the worker (single-worker per pod), so
/// it's no longer on AppContext.
pub struct AppContext {
    pub filter_mode: TeamFilterMode,
    pub filtered_teams: TeamList,
    pub flush_interval: Duration,
    pub max_entries_per_partition: usize,
}

impl AppContext {
    pub fn new(config: &Config) -> Self {
        Self {
            filter_mode: config.filter_mode,
            filtered_teams: config.filtered_teams.clone(),
            flush_interval: Duration::from_secs(config.flush_interval_secs),
            max_entries_per_partition: config.max_entries_per_partition,
        }
    }

    pub fn should_process(&self, team_id: i64) -> bool {
        self.filter_mode
            .should_process(&self.filtered_teams.teams, team_id)
    }
}
