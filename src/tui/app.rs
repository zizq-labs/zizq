// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! TUI application state model.

use crate::admin::{AdminJobSummary, JobChangeStatus};

use super::events::Event;

/// Connection status to the admin API.
pub enum ConnectionStatus {
    Connecting,
    Connected,
    Disconnected,
}

/// Top-level application state for the TUI.
pub struct App {
    pub status: ConnectionStatus,
    pub server_version: Option<String>,
    pub server_uptime_ms: Option<u64>,
    pub ready_jobs: Vec<AdminJobSummary>,
    pub working_jobs: Vec<AdminJobSummary>,
    pub now_ms: u64,
}

impl App {
    pub fn new() -> Self {
        Self {
            status: ConnectionStatus::Connecting,
            server_version: None,
            server_uptime_ms: None,
            ready_jobs: Vec::new(),
            working_jobs: Vec::new(),
            now_ms: 0,
        }
    }

    /// Handle an incoming event, updating state.
    ///
    /// Returns `true` if the application should quit.
    pub fn handle_event(&mut self, event: Event) -> bool {
        match event {
            Event::Quit => return true,
            Event::ServerConnecting => {
                self.status = ConnectionStatus::Connecting;
            }
            Event::ServerConnected => {
                self.status = ConnectionStatus::Connected;
            }
            Event::ServerHeartbeat { version, uptime_ms } => {
                self.status = ConnectionStatus::Connected;
                self.server_version = Some(version);
                self.server_uptime_ms = Some(uptime_ms);
            }
            Event::ServerJobSnapshot { ready, working } => {
                self.ready_jobs = ready;
                self.working_jobs = working;
            }
            Event::ServerJobChanged { id, status, job } => match status {
                JobChangeStatus::Ready => {
                    if let Some(job) = job {
                        let key = (job.priority, &job.id);
                        // Determine where in the list the job should sit.
                        let pos = self
                            .ready_jobs
                            .partition_point(|j| (j.priority, &j.id) < key);
                        // Insert the element at this position (unless it would
                        // be a duplicate).
                        if self.ready_jobs.get(pos).is_none_or(|j| j.id != id) {
                            self.ready_jobs.insert(pos, job);
                        }
                    }
                }
                JobChangeStatus::Working => {
                    self.ready_jobs.retain(|j| j.id != id);
                    if let Some(job) = job {
                        let dequeued = job.dequeued_at.unwrap_or(0);
                        // Determine where in the list the job should sit.
                        let pos = self
                            .working_jobs
                            .partition_point(|j| j.dequeued_at.unwrap_or(0) < dequeued);
                        // Insert the element at this position (unless it would
                        // be a duplicate).
                        if self.working_jobs.get(pos).is_none_or(|j| j.id != id) {
                            self.working_jobs.insert(pos, job);
                        }
                    }
                }
                JobChangeStatus::ReadyRemoved => {
                    self.ready_jobs.retain(|j| j.id != id);
                }
                JobChangeStatus::WorkingRemoved => {
                    self.working_jobs.retain(|j| j.id != id);
                }
                JobChangeStatus::Completed | JobChangeStatus::Dead => {
                    self.ready_jobs.retain(|j| j.id != id);
                    self.working_jobs.retain(|j| j.id != id);
                }
            },
            Event::ServerDisconnected => {
                self.status = ConnectionStatus::Disconnected;
                self.server_version = None;
                self.server_uptime_ms = None;
                self.ready_jobs.clear();
                self.working_jobs.clear();
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job(id: &str, priority: u16, dequeued_at: Option<u64>) -> AdminJobSummary {
        AdminJobSummary {
            id: id.to_string(),
            queue: "q".to_string(),
            job_type: "t".to_string(),
            priority,
            ready_at: 1000,
            attempts: 0,
            dequeued_at,
            failed_at: None,
        }
    }

    fn ready_event(id: &str, priority: u16) -> Event {
        Event::ServerJobChanged {
            id: id.to_string(),
            status: JobChangeStatus::Ready,
            job: Some(job(id, priority, None)),
        }
    }

    fn working_event(id: &str, dequeued_at: u64) -> Event {
        Event::ServerJobChanged {
            id: id.to_string(),
            status: JobChangeStatus::Working,
            job: Some(job(id, 0, Some(dequeued_at))),
        }
    }

    fn ids(jobs: &[AdminJobSummary]) -> Vec<&str> {
        jobs.iter().map(|j| j.id.as_str()).collect()
    }

    // ── Quit ────────────────────────────────────────────────────────

    #[test]
    fn quit_returns_true() {
        let mut app = App::new();
        assert!(app.handle_event(Event::Quit));
    }

    // ── Connection lifecycle ────────────────────────────────────────

    #[test]
    fn heartbeat_sets_connected_and_metadata() {
        let mut app = App::new();
        app.handle_event(Event::ServerHeartbeat {
            version: "1.0.0".to_string(),
            uptime_ms: 5000,
        });
        assert!(matches!(app.status, ConnectionStatus::Connected));
        assert_eq!(app.server_version.as_deref(), Some("1.0.0"));
        assert_eq!(app.server_uptime_ms, Some(5000));
    }

    #[test]
    fn disconnect_clears_state() {
        let mut app = App::new();
        app.handle_event(Event::ServerHeartbeat {
            version: "1.0.0".to_string(),
            uptime_ms: 5000,
        });
        app.handle_event(ready_event("j1", 0));
        app.handle_event(Event::ServerDisconnected);

        assert!(matches!(app.status, ConnectionStatus::Disconnected));
        assert!(app.server_version.is_none());
        assert!(app.server_uptime_ms.is_none());
        assert!(app.ready_jobs.is_empty());
        assert!(app.working_jobs.is_empty());
    }

    // ── Snapshot ────────────────────────────────────────────────────

    #[test]
    fn snapshot_replaces_jobs() {
        let mut app = App::new();
        app.handle_event(ready_event("old", 0));

        app.handle_event(Event::ServerJobSnapshot {
            ready: vec![job("r1", 0, None)],
            working: vec![job("w1", 0, Some(100))],
        });

        assert_eq!(ids(&app.ready_jobs), vec!["r1"]);
        assert_eq!(ids(&app.working_jobs), vec!["w1"]);
    }

    // ── Ready insertion ─────────────────────────────────────────────

    #[test]
    fn ready_inserts_in_priority_order() {
        let mut app = App::new();
        app.handle_event(ready_event("low", 5));
        app.handle_event(ready_event("high", 1));
        app.handle_event(ready_event("mid", 3));

        assert_eq!(ids(&app.ready_jobs), vec!["high", "mid", "low"]);
    }

    #[test]
    fn ready_deduplicates() {
        let mut app = App::new();
        app.handle_event(ready_event("j1", 0));
        app.handle_event(ready_event("j1", 0));

        assert_eq!(app.ready_jobs.len(), 1);
    }

    // ── Working insertion ───────────────────────────────────────────

    #[test]
    fn working_inserts_in_dequeued_order() {
        let mut app = App::new();
        app.handle_event(working_event("w2", 200));
        app.handle_event(working_event("w1", 100));
        app.handle_event(working_event("w3", 300));

        assert_eq!(ids(&app.working_jobs), vec!["w1", "w2", "w3"]);
    }

    #[test]
    fn working_removes_from_ready() {
        let mut app = App::new();
        app.handle_event(ready_event("j1", 0));
        assert_eq!(app.ready_jobs.len(), 1);

        app.handle_event(working_event("j1", 100));
        assert!(app.ready_jobs.is_empty());
        assert_eq!(ids(&app.working_jobs), vec!["j1"]);
    }

    #[test]
    fn working_deduplicates() {
        let mut app = App::new();
        app.handle_event(working_event("j1", 100));
        app.handle_event(working_event("j1", 100));

        assert_eq!(app.working_jobs.len(), 1);
    }

    // ── Removals ────────────────────────────────────────────────────

    #[test]
    fn ready_removed_removes_from_ready() {
        let mut app = App::new();
        app.handle_event(ready_event("j1", 0));
        app.handle_event(ready_event("j2", 1));

        app.handle_event(Event::ServerJobChanged {
            id: "j1".to_string(),
            status: JobChangeStatus::ReadyRemoved,
            job: None,
        });

        assert_eq!(ids(&app.ready_jobs), vec!["j2"]);
    }

    #[test]
    fn working_removed_removes_from_working() {
        let mut app = App::new();
        app.handle_event(working_event("j1", 100));
        app.handle_event(working_event("j2", 200));

        app.handle_event(Event::ServerJobChanged {
            id: "j1".to_string(),
            status: JobChangeStatus::WorkingRemoved,
            job: None,
        });

        assert_eq!(ids(&app.working_jobs), vec!["j2"]);
    }

    #[test]
    fn completed_removes_from_both() {
        let mut app = App::new();
        app.handle_event(ready_event("r1", 0));
        app.handle_event(working_event("w1", 100));

        app.handle_event(Event::ServerJobChanged {
            id: "r1".to_string(),
            status: JobChangeStatus::Completed,
            job: None,
        });
        app.handle_event(Event::ServerJobChanged {
            id: "w1".to_string(),
            status: JobChangeStatus::Dead,
            job: None,
        });

        assert!(app.ready_jobs.is_empty());
        assert!(app.working_jobs.is_empty());
    }

    #[test]
    fn removal_of_unknown_id_is_noop() {
        let mut app = App::new();
        app.handle_event(ready_event("j1", 0));

        app.handle_event(Event::ServerJobChanged {
            id: "nonexistent".to_string(),
            status: JobChangeStatus::ReadyRemoved,
            job: None,
        });

        assert_eq!(ids(&app.ready_jobs), vec!["j1"]);
    }
}
