use std::hash::{Hash, Hasher};
use std::time::Duration;

use siphasher::sip::SipHasher13;

use crate::config::{Config, TeamFilterMode, TeamList};

pub struct AppContext {
    pub filter_mode: TeamFilterMode,
    pub filtered_teams: TeamList,
    pub rollout_percentage: u8,
    pub flush_interval: Duration,
    pub max_entries_per_partition: usize,
}

impl AppContext {
    pub fn new(config: &Config) -> Self {
        Self {
            filter_mode: config.filter_mode,
            filtered_teams: config.filtered_teams.clone(),
            rollout_percentage: config.rollout_percentage,
            flush_interval: Duration::from_secs(config.flush_interval_secs),
            max_entries_per_partition: config.max_entries_per_partition,
        }
    }

    pub fn should_process(&self, team_id: i64) -> bool {
        match self.filter_mode {
            // Opt-in: only teams explicitly named. Rollout is ignored because
            // the list itself is the rollout in this mode.
            TeamFilterMode::OptIn => self.filtered_teams.teams.contains(&team_id),
            // Opt-out: process every team except those explicitly named,
            // further narrowed by the rollout percentage.
            TeamFilterMode::OptOut => {
                !self.filtered_teams.teams.contains(&team_id) && self.team_in_rollout(team_id)
            }
        }
    }

    fn team_in_rollout(&self, team_id: i64) -> bool {
        if self.rollout_percentage >= 100 {
            return true;
        }
        if self.rollout_percentage == 0 {
            return false;
        }
        // SipHash13 with a fixed (zeroed) key gives a stable bucket assignment
        // across pods and restarts: same team always lands in the same bucket
        // at a given percentage, so ramps are deterministic.
        let mut hasher = SipHasher13::new();
        team_id.hash(&mut hasher);
        (hasher.finish() % 100) < self.rollout_percentage as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(filter_mode: TeamFilterMode, teams: Vec<i64>, rollout_percentage: u8) -> AppContext {
        AppContext {
            filter_mode,
            filtered_teams: TeamList { teams },
            rollout_percentage,
            flush_interval: Duration::from_secs(0),
            max_entries_per_partition: 0,
        }
    }

    #[test]
    fn opt_in_only_processes_listed_teams() {
        let c = ctx(TeamFilterMode::OptIn, vec![2], 100);
        assert!(c.should_process(2));
        assert!(!c.should_process(3));
        assert!(!c.should_process(999));
    }

    #[test]
    fn opt_in_ignores_rollout_percentage() {
        // Team 2 is explicitly listed, so it must be processed regardless
        // of rollout. The opt-in list is the rollout in this mode.
        let c = ctx(TeamFilterMode::OptIn, vec![2], 0);
        assert!(c.should_process(2));
    }

    #[test]
    fn opt_out_with_empty_list_and_full_rollout_processes_all() {
        let c = ctx(TeamFilterMode::OptOut, vec![], 100);
        for team in [1, 2, 999, 12345] {
            assert!(c.should_process(team), "should process team {team}");
        }
    }

    #[test]
    fn opt_out_zero_rollout_drops_everyone() {
        let c = ctx(TeamFilterMode::OptOut, vec![], 0);
        for team in [1, 2, 999] {
            assert!(!c.should_process(team));
        }
    }

    #[test]
    fn opt_out_blocks_blacklisted_teams_regardless_of_rollout() {
        let c = ctx(TeamFilterMode::OptOut, vec![999], 100);
        assert!(!c.should_process(999));
        assert!(c.should_process(1));
    }

    #[test]
    fn rollout_is_deterministic_per_team_id() {
        let c = ctx(TeamFilterMode::OptOut, vec![], 50);
        let first = c.should_process(12345);
        for _ in 0..100 {
            assert_eq!(c.should_process(12345), first);
        }
    }

    #[test]
    fn rollout_percentage_approximates_target_share() {
        // 10% rollout across 10k synthetic team_ids should land within a
        // few percentage points of 1000. SipHash + modulo is well-behaved
        // enough that the tolerance can be tight without being flaky.
        let c = ctx(TeamFilterMode::OptOut, vec![], 10);
        let included = (1..=10_000).filter(|t| c.should_process(*t)).count();
        assert!(
            (900..=1100).contains(&included),
            "expected ~1000 of 10000 at 10%, got {included}"
        );
    }
}
