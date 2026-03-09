// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! TUI application state model.

use crate::admin::{AdminJobSummary, JobChangeStatus, ServerStatus};
use crate::license::Tier;

use super::events::Event;

/// Active tab in the TUI.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tab {
    Ready,
    InFlight,
    Scheduled,
}

impl Tab {
    const ALL: [Tab; 3] = [Tab::InFlight, Tab::Ready, Tab::Scheduled];

    pub fn next(self) -> Tab {
        let idx = Self::ALL.iter().position(|&t| t == self).unwrap();
        Self::ALL[(idx + 1) % Self::ALL.len()]
    }

    pub fn prev(self) -> Tab {
        let idx = Self::ALL.iter().position(|&t| t == self).unwrap();
        Self::ALL[(idx + Self::ALL.len() - 1) % Self::ALL.len()]
    }
}

/// Connection status to the admin API.
pub enum ConnectionStatus {
    Connecting,
    Connected,
    Disconnected,
}

/// Top-level application state for the TUI.
pub struct App {
    pub host: String,
    pub status: ConnectionStatus,
    pub server_version: Option<String>,
    pub server_uptime_ms: Option<u64>,
    pub server_tier: Option<Tier>,
    pub total_ready: usize,
    pub total_in_flight: usize,
    pub total_scheduled: usize,
    pub ready_jobs: Vec<AdminJobSummary>,
    pub in_flight_jobs: Vec<AdminJobSummary>,
    pub scheduled_jobs: Vec<AdminJobSummary>,
    pub now_ms: u64,
    pub active_tab: Tab,
}

impl App {
    pub fn new(host: String) -> Self {
        Self {
            host,
            status: ConnectionStatus::Connecting,
            server_version: None,
            server_uptime_ms: None,
            server_tier: None,
            total_ready: 0,
            total_in_flight: 0,
            total_scheduled: 0,
            ready_jobs: Vec::new(),
            in_flight_jobs: Vec::new(),
            scheduled_jobs: Vec::new(),
            now_ms: 0,
            active_tab: Tab::InFlight,
        }
    }

    /// Apply server status fields from any message.
    fn apply_server_status(&mut self, server: ServerStatus) {
        self.status = ConnectionStatus::Connected;
        self.server_version = Some(server.version);
        self.server_uptime_ms = Some(server.uptime_ms);
        self.server_tier = Tier::parse(&server.tier);
        self.total_ready = server.total_ready;
        self.total_in_flight = server.total_in_flight;
        self.total_scheduled = server.total_scheduled;
    }

    /// Handle an incoming event, updating state.
    ///
    /// Returns `true` if the application should quit.
    pub fn handle_event(&mut self, event: Event) -> bool {
        match event {
            Event::Quit => return true,
            Event::NextTab => {
                self.active_tab = self.active_tab.next();
            }
            Event::PrevTab => {
                self.active_tab = self.active_tab.prev();
            }
            Event::ServerConnecting => {
                self.status = ConnectionStatus::Connecting;
            }
            Event::ServerConnected => {
                self.status = ConnectionStatus::Connected;
            }
            Event::ServerHeartbeat { server } => {
                self.apply_server_status(server);
            }
            Event::ServerJobSnapshot {
                server,
                ready,
                in_flight,
                scheduled,
            } => {
                self.apply_server_status(server);
                self.ready_jobs = ready;
                self.in_flight_jobs = in_flight;
                self.scheduled_jobs = scheduled;
            }
            Event::ServerJobChanged {
                server,
                id,
                status,
                job,
            } => {
                self.apply_server_status(server);
                match status {
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
                    JobChangeStatus::InFlight => {
                        self.ready_jobs.retain(|j| j.id != id);
                        if let Some(job) = job {
                            let dequeued = job.dequeued_at.unwrap_or(0);
                            // Determine where in the list the job should sit.
                            let pos = self
                                .in_flight_jobs
                                .partition_point(|j| j.dequeued_at.unwrap_or(0) < dequeued);
                            // Insert the element at this position (unless it would
                            // be a duplicate).
                            if self.in_flight_jobs.get(pos).is_none_or(|j| j.id != id) {
                                self.in_flight_jobs.insert(pos, job);
                            }
                        }
                    }
                    JobChangeStatus::ReadyRemoved => {
                        self.ready_jobs.retain(|j| j.id != id);
                    }
                    JobChangeStatus::InFlightRemoved => {
                        self.in_flight_jobs.retain(|j| j.id != id);
                    }
                    JobChangeStatus::Scheduled => {
                        if let Some(job) = job {
                            let ready_at = job.ready_at;
                            let pos = self
                                .scheduled_jobs
                                .partition_point(|j| (j.ready_at, &j.id) < (ready_at, &job.id));
                            if self.scheduled_jobs.get(pos).is_none_or(|j| j.id != id) {
                                self.scheduled_jobs.insert(pos, job);
                            }
                        }
                    }
                    JobChangeStatus::ScheduledRemoved => {
                        self.scheduled_jobs.retain(|j| j.id != id);
                    }
                    JobChangeStatus::Completed | JobChangeStatus::Dead => {
                        self.ready_jobs.retain(|j| j.id != id);
                        self.in_flight_jobs.retain(|j| j.id != id);
                        self.scheduled_jobs.retain(|j| j.id != id);
                    }
                }
            }
            Event::ServerDisconnected => {
                self.status = ConnectionStatus::Disconnected;
                self.server_version = None;
                self.server_uptime_ms = None;
                self.server_tier = None;
                self.total_ready = 0;
                self.total_in_flight = 0;
                self.total_scheduled = 0;
                self.ready_jobs.clear();
                self.in_flight_jobs.clear();
                self.scheduled_jobs.clear();
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_server() -> crate::admin::ServerStatus {
        crate::admin::ServerStatus {
            version: "1.0.0".to_string(),
            uptime_ms: 5000,
            tier: "pro".to_string(),
            total_ready: 0,
            total_in_flight: 0,
            total_scheduled: 0,
        }
    }

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
            server: default_server(),
            id: id.to_string(),
            status: JobChangeStatus::Ready,
            job: Some(job(id, priority, None)),
        }
    }

    fn in_flight_event(id: &str, dequeued_at: u64) -> Event {
        Event::ServerJobChanged {
            server: default_server(),
            id: id.to_string(),
            status: JobChangeStatus::InFlight,
            job: Some(job(id, 0, Some(dequeued_at))),
        }
    }

    fn ids(jobs: &[AdminJobSummary]) -> Vec<&str> {
        jobs.iter().map(|j| j.id.as_str()).collect()
    }

    // ── Quit ────────────────────────────────────────────────────────

    #[test]
    fn quit_returns_true() {
        let mut app = App::new("127.0.0.1:8901".into());
        assert!(app.handle_event(Event::Quit));
    }

    // ── Connection lifecycle ────────────────────────────────────────

    #[test]
    fn heartbeat_sets_connected_and_metadata() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.handle_event(Event::ServerHeartbeat {
            server: default_server(),
        });
        assert!(matches!(app.status, ConnectionStatus::Connected));
        assert_eq!(app.server_version.as_deref(), Some("1.0.0"));
        assert_eq!(app.server_uptime_ms, Some(5000));
    }

    #[test]
    fn disconnect_clears_state() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.handle_event(Event::ServerHeartbeat {
            server: default_server(),
        });
        app.handle_event(ready_event("j1", 0));
        app.handle_event(Event::ServerDisconnected);

        assert!(matches!(app.status, ConnectionStatus::Disconnected));
        assert!(app.server_version.is_none());
        assert!(app.server_uptime_ms.is_none());
        assert_eq!(app.total_ready, 0);
        assert_eq!(app.total_in_flight, 0);
        assert_eq!(app.total_scheduled, 0);
        assert!(app.ready_jobs.is_empty());
        assert!(app.in_flight_jobs.is_empty());
        assert!(app.scheduled_jobs.is_empty());
    }

    // ── Snapshot ────────────────────────────────────────────────────

    #[test]
    fn snapshot_replaces_jobs() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.handle_event(ready_event("old", 0));

        app.handle_event(Event::ServerJobSnapshot {
            server: default_server(),
            ready: vec![job("r1", 0, None)],
            in_flight: vec![job("w1", 0, Some(100))],
            scheduled: vec![job("s1", 0, None)],
        });

        assert_eq!(ids(&app.ready_jobs), vec!["r1"]);
        assert_eq!(ids(&app.in_flight_jobs), vec!["w1"]);
        assert_eq!(ids(&app.scheduled_jobs), vec!["s1"]);
    }

    // ── Ready insertion ─────────────────────────────────────────────

    #[test]
    fn ready_inserts_in_priority_order() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.handle_event(ready_event("low", 5));
        app.handle_event(ready_event("high", 1));
        app.handle_event(ready_event("mid", 3));

        assert_eq!(ids(&app.ready_jobs), vec!["high", "mid", "low"]);
    }

    #[test]
    fn ready_deduplicates() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.handle_event(ready_event("j1", 0));
        app.handle_event(ready_event("j1", 0));

        assert_eq!(app.ready_jobs.len(), 1);
    }

    // ── In-flight insertion ──────────────────────────────────────────

    #[test]
    fn in_flight_inserts_in_dequeued_order() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.handle_event(in_flight_event("w2", 200));
        app.handle_event(in_flight_event("w1", 100));
        app.handle_event(in_flight_event("w3", 300));

        assert_eq!(ids(&app.in_flight_jobs), vec!["w1", "w2", "w3"]);
    }

    #[test]
    fn in_flight_removes_from_ready() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.handle_event(ready_event("j1", 0));
        assert_eq!(app.ready_jobs.len(), 1);

        app.handle_event(in_flight_event("j1", 100));
        assert!(app.ready_jobs.is_empty());
        assert_eq!(ids(&app.in_flight_jobs), vec!["j1"]);
    }

    #[test]
    fn in_flight_deduplicates() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.handle_event(in_flight_event("j1", 100));
        app.handle_event(in_flight_event("j1", 100));

        assert_eq!(app.in_flight_jobs.len(), 1);
    }

    // ── Removals ────────────────────────────────────────────────────

    #[test]
    fn ready_removed_removes_from_ready() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.handle_event(ready_event("j1", 0));
        app.handle_event(ready_event("j2", 1));

        app.handle_event(Event::ServerJobChanged {
            server: default_server(),
            id: "j1".to_string(),
            status: JobChangeStatus::ReadyRemoved,
            job: None,
        });

        assert_eq!(ids(&app.ready_jobs), vec!["j2"]);
    }

    #[test]
    fn in_flight_removed_removes_from_in_flight() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.handle_event(in_flight_event("j1", 100));
        app.handle_event(in_flight_event("j2", 200));

        app.handle_event(Event::ServerJobChanged {
            server: default_server(),
            id: "j1".to_string(),
            status: JobChangeStatus::InFlightRemoved,
            job: None,
        });

        assert_eq!(ids(&app.in_flight_jobs), vec!["j2"]);
    }

    #[test]
    fn completed_removes_from_both() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.handle_event(ready_event("r1", 0));
        app.handle_event(in_flight_event("w1", 100));

        app.handle_event(Event::ServerJobChanged {
            server: default_server(),
            id: "r1".to_string(),
            status: JobChangeStatus::Completed,
            job: None,
        });
        app.handle_event(Event::ServerJobChanged {
            server: default_server(),
            id: "w1".to_string(),
            status: JobChangeStatus::Dead,
            job: None,
        });

        assert!(app.ready_jobs.is_empty());
        assert!(app.in_flight_jobs.is_empty());
    }

    #[test]
    fn removal_of_unknown_id_is_noop() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.handle_event(ready_event("j1", 0));

        app.handle_event(Event::ServerJobChanged {
            server: default_server(),
            id: "nonexistent".to_string(),
            status: JobChangeStatus::ReadyRemoved,
            job: None,
        });

        assert_eq!(ids(&app.ready_jobs), vec!["j1"]);
    }

    // ── Tab switching ───────────────────────────────────────────────

    #[test]
    fn default_tab_is_in_flight() {
        let app = App::new("127.0.0.1:8901".into());
        assert_eq!(app.active_tab, Tab::InFlight);
    }

    #[test]
    fn next_tab_cycles_forward() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.handle_event(Event::NextTab);
        assert_eq!(app.active_tab, Tab::Ready);
    }

    #[test]
    fn next_tab_cycles_through_scheduled() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.handle_event(Event::NextTab);
        app.handle_event(Event::NextTab);
        assert_eq!(app.active_tab, Tab::Scheduled);
    }

    #[test]
    fn next_tab_wraps_around() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.handle_event(Event::NextTab);
        app.handle_event(Event::NextTab);
        app.handle_event(Event::NextTab);
        assert_eq!(app.active_tab, Tab::InFlight);
    }

    #[test]
    fn prev_tab_cycles_backward() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.handle_event(Event::NextTab);
        app.handle_event(Event::PrevTab);
        assert_eq!(app.active_tab, Tab::InFlight);
    }

    #[test]
    fn prev_tab_wraps_around() {
        let mut app = App::new("127.0.0.1:8901".into());
        app.handle_event(Event::PrevTab);
        assert_eq!(app.active_tab, Tab::Scheduled);
    }

    // ── Scheduled insertion ────────────────────────────────────────

    #[test]
    fn scheduled_inserts_in_ready_at_order() {
        let mut app = App::new("127.0.0.1:8901".into());
        let scheduled_event = |id: &str, ready_at: u64| -> Event {
            Event::ServerJobChanged {
                server: default_server(),
                id: id.to_string(),
                status: JobChangeStatus::Scheduled,
                job: Some(AdminJobSummary {
                    id: id.to_string(),
                    queue: "q".to_string(),
                    job_type: "t".to_string(),
                    priority: 0,
                    ready_at,
                    attempts: 0,
                    dequeued_at: None,
                    failed_at: None,
                }),
            }
        };

        app.handle_event(scheduled_event("s2", 2000));
        app.handle_event(scheduled_event("s1", 1000));
        app.handle_event(scheduled_event("s3", 3000));

        assert_eq!(ids(&app.scheduled_jobs), vec!["s1", "s2", "s3"]);
    }

    #[test]
    fn scheduled_removed_removes_from_scheduled() {
        let mut app = App::new("127.0.0.1:8901".into());
        let scheduled_event = |id: &str, ready_at: u64| -> Event {
            Event::ServerJobChanged {
                server: default_server(),
                id: id.to_string(),
                status: JobChangeStatus::Scheduled,
                job: Some(AdminJobSummary {
                    id: id.to_string(),
                    queue: "q".to_string(),
                    job_type: "t".to_string(),
                    priority: 0,
                    ready_at,
                    attempts: 0,
                    dequeued_at: None,
                    failed_at: None,
                }),
            }
        };

        app.handle_event(scheduled_event("s1", 1000));
        app.handle_event(scheduled_event("s2", 2000));

        app.handle_event(Event::ServerJobChanged {
            server: default_server(),
            id: "s1".to_string(),
            status: JobChangeStatus::ScheduledRemoved,
            job: None,
        });

        assert_eq!(ids(&app.scheduled_jobs), vec!["s2"]);
    }

    #[test]
    fn completed_removes_from_scheduled() {
        let mut app = App::new("127.0.0.1:8901".into());
        let scheduled_event = |id: &str, ready_at: u64| -> Event {
            Event::ServerJobChanged {
                server: default_server(),
                id: id.to_string(),
                status: JobChangeStatus::Scheduled,
                job: Some(AdminJobSummary {
                    id: id.to_string(),
                    queue: "q".to_string(),
                    job_type: "t".to_string(),
                    priority: 0,
                    ready_at,
                    attempts: 0,
                    dequeued_at: None,
                    failed_at: None,
                }),
            }
        };

        app.handle_event(scheduled_event("s1", 1000));

        app.handle_event(Event::ServerJobChanged {
            server: default_server(),
            id: "s1".to_string(),
            status: JobChangeStatus::Completed,
            job: None,
        });

        assert!(app.scheduled_jobs.is_empty());
    }
}
