use std::hash::{Hash, Hasher};
use std::time::Duration;

use siphasher::sip::SipHasher13;

use crate::config::{Config, TeamList};

pub struct AppContext {
    pub allowed_teams: TeamList,
    pub blocked_teams: TeamList,
    pub rollout_percentage: u8,
    pub flush_interval: Duration,
    pub max_entries_per_partition: usize,
}

impl AppContext {
    pub fn new(config: &Config) -> Self {
        Self {
            allowed_teams: config.allowed_teams.clone(),
            blocked_teams: config.blocked_teams.clone(),
            rollout_percentage: config.rollout_percentage,
            flush_interval: Duration::from_secs(config.flush_interval_secs),
            max_entries_per_partition: config.max_entries_per_partition,
        }
    }

    pub fn should_process(&self, team_id: i64) -> bool {
        if self.blocked_teams.teams.contains(&team_id) {
            return false;
        }
        if self.allowed_teams.teams.contains(&team_id) {
            return true;
        }
        self.team_in_rollout(team_id)
    }

    fn team_in_rollout(&self, team_id: i64) -> bool {
        if self.rollout_percentage >= 100 {
            return true;
        }
        if self.rollout_percentage == 0 {
            return false;
        }
        let mut hasher = SipHasher13::new();
        team_id.hash(&mut hasher);
        (hasher.finish() % 100) < self.rollout_percentage as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(allowed: Vec<i64>, blocked: Vec<i64>, rollout_percentage: u8) -> AppContext {
        AppContext {
            allowed_teams: TeamList { teams: allowed },
            blocked_teams: TeamList { teams: blocked },
            rollout_percentage,
            flush_interval: Duration::from_secs(0),
            max_entries_per_partition: 0,
        }
    }

    #[test]
    fn empty_lists_full_rollout_processes_all() {
        let c = ctx(vec![], vec![], 100);
        for team in [1, 2, 999, 12345] {
            assert!(c.should_process(team), "should process team {team}");
        }
    }

    #[test]
    fn empty_lists_zero_rollout_drops_all() {
        let c = ctx(vec![], vec![], 0);
        for team in [1, 2, 999] {
            assert!(!c.should_process(team));
        }
    }

    #[test]
    fn allowed_team_always_processes_even_at_zero_rollout() {
        let c = ctx(vec![2], vec![], 0);
        assert!(c.should_process(2));
        assert!(!c.should_process(3));
    }

    #[test]
    fn blocked_team_never_processes_even_at_full_rollout() {
        let c = ctx(vec![], vec![999], 100);
        assert!(!c.should_process(999));
        assert!(c.should_process(1));
    }

    #[test]
    fn block_list_overrides_allow_list_on_overlap() {
        let c = ctx(vec![2], vec![2], 100);
        assert!(!c.should_process(2));
    }

    #[test]
    fn allowed_team_processes_at_partial_rollout_when_hash_misses() {
        let c = ctx(vec![999_999], vec![], 1);
        assert!(c.should_process(999_999));
    }

    #[test]
    fn rollout_is_deterministic_per_team_id() {
        let c = ctx(vec![], vec![], 50);
        let first = c.should_process(12345);
        for _ in 0..100 {
            assert_eq!(c.should_process(12345), first);
        }
    }

    #[test]
    fn rollout_percentage_approximates_target_share() {
        let c = ctx(vec![], vec![], 10);
        let included = (1..=10_000).filter(|t| c.should_process(*t)).count();
        assert!(
            (900..=1100).contains(&included),
            "expected ~1000 of 10000 at 10%, got {included}"
        );
    }
}
