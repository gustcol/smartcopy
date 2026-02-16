//! Bandwidth scheduling module
//!
//! Provides time-based bandwidth limits for transfers.
//! Useful for scheduling high-bandwidth transfers during off-peak hours
//! or limiting bandwidth during business hours.

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use chrono::{Local, Timelike, Weekday, Datelike};

/// A time-based bandwidth schedule
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BandwidthSchedule {
    /// Schedule rules (evaluated in order, first match wins)
    pub rules: Vec<ScheduleRule>,
    /// Default bandwidth limit when no rules match (bytes/sec, 0 = unlimited)
    pub default_limit: u64,
    /// Whether to apply schedule (can be disabled temporarily)
    pub enabled: bool,
}

/// A single schedule rule
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleRule {
    /// Rule name for identification
    pub name: String,
    /// Days of week this rule applies (None = all days)
    pub days: Option<Vec<DayOfWeek>>,
    /// Start time (hour:minute in 24h format)
    pub start_time: TimeOfDay,
    /// End time (hour:minute in 24h format)
    pub end_time: TimeOfDay,
    /// Bandwidth limit in bytes/sec (0 = unlimited)
    pub limit: u64,
    /// Priority (higher = evaluated first)
    pub priority: i32,
}

/// Day of week
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DayOfWeek {
    Monday,
    Tuesday,
    Wednesday,
    Thursday,
    Friday,
    Saturday,
    Sunday,
}

impl DayOfWeek {
    fn from_chrono(weekday: Weekday) -> Self {
        match weekday {
            Weekday::Mon => DayOfWeek::Monday,
            Weekday::Tue => DayOfWeek::Tuesday,
            Weekday::Wed => DayOfWeek::Wednesday,
            Weekday::Thu => DayOfWeek::Thursday,
            Weekday::Fri => DayOfWeek::Friday,
            Weekday::Sat => DayOfWeek::Saturday,
            Weekday::Sun => DayOfWeek::Sunday,
        }
    }
}

/// Time of day
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct TimeOfDay {
    pub hour: u32,
    pub minute: u32,
}

impl TimeOfDay {
    pub fn new(hour: u32, minute: u32) -> Self {
        Self {
            hour: hour.min(23),
            minute: minute.min(59),
        }
    }

    pub fn from_hhmm(hhmm: &str) -> Option<Self> {
        let parts: Vec<&str> = hhmm.split(':').collect();
        if parts.len() != 2 {
            return None;
        }
        let hour = parts[0].parse().ok()?;
        let minute = parts[1].parse().ok()?;
        Some(Self::new(hour, minute))
    }

    fn to_minutes(&self) -> u32 {
        self.hour * 60 + self.minute
    }
}

impl BandwidthSchedule {
    /// Create a new empty schedule
    pub fn new() -> Self {
        Self {
            rules: Vec::new(),
            default_limit: 0,
            enabled: true,
        }
    }

    /// Create a common schedule: full speed at night, limited during day
    pub fn business_hours(day_limit: u64, night_limit: u64) -> Self {
        Self {
            rules: vec![
                ScheduleRule {
                    name: "Business hours".to_string(),
                    days: Some(vec![
                        DayOfWeek::Monday,
                        DayOfWeek::Tuesday,
                        DayOfWeek::Wednesday,
                        DayOfWeek::Thursday,
                        DayOfWeek::Friday,
                    ]),
                    start_time: TimeOfDay::new(9, 0),
                    end_time: TimeOfDay::new(18, 0),
                    limit: day_limit,
                    priority: 10,
                },
                ScheduleRule {
                    name: "Night hours".to_string(),
                    days: None,
                    start_time: TimeOfDay::new(22, 0),
                    end_time: TimeOfDay::new(6, 0),
                    limit: night_limit,
                    priority: 5,
                },
            ],
            default_limit: day_limit,
            enabled: true,
        }
    }

    /// Create weekend-only full speed schedule
    pub fn weekend_full_speed(weekday_limit: u64) -> Self {
        Self {
            rules: vec![
                ScheduleRule {
                    name: "Weekend".to_string(),
                    days: Some(vec![DayOfWeek::Saturday, DayOfWeek::Sunday]),
                    start_time: TimeOfDay::new(0, 0),
                    end_time: TimeOfDay::new(23, 59),
                    limit: 0, // unlimited
                    priority: 10,
                },
            ],
            default_limit: weekday_limit,
            enabled: true,
        }
    }

    /// Add a rule to the schedule
    pub fn add_rule(&mut self, rule: ScheduleRule) {
        self.rules.push(rule);
        self.rules.sort_by(|a, b| b.priority.cmp(&a.priority));
    }

    /// Get current bandwidth limit based on time
    pub fn current_limit(&self) -> u64 {
        if !self.enabled {
            return 0; // Unlimited when disabled
        }

        let now = Local::now();
        let current_day = DayOfWeek::from_chrono(now.weekday());
        let current_time = TimeOfDay::new(now.hour(), now.minute());

        for rule in &self.rules {
            if self.rule_matches(rule, current_day, current_time) {
                return rule.limit;
            }
        }

        self.default_limit
    }

    /// Check if a rule matches current time
    fn rule_matches(&self, rule: &ScheduleRule, day: DayOfWeek, time: TimeOfDay) -> bool {
        // Check day
        if let Some(ref days) = rule.days {
            if !days.contains(&day) {
                return false;
            }
        }

        // Check time (handle overnight spans)
        let current_mins = time.to_minutes();
        let start_mins = rule.start_time.to_minutes();
        let end_mins = rule.end_time.to_minutes();

        if start_mins <= end_mins {
            // Normal span (e.g., 09:00 - 18:00)
            current_mins >= start_mins && current_mins < end_mins
        } else {
            // Overnight span (e.g., 22:00 - 06:00)
            current_mins >= start_mins || current_mins < end_mins
        }
    }

    /// Get next schedule change time
    pub fn next_change(&self) -> Option<Duration> {
        if !self.enabled || self.rules.is_empty() {
            return None;
        }

        let now = Local::now();
        let current_mins = now.hour() * 60 + now.minute();

        let mut min_wait: Option<u32> = None;

        for rule in &self.rules {
            let start_mins = rule.start_time.to_minutes();
            let end_mins = rule.end_time.to_minutes();

            for boundary in [start_mins, end_mins] {
                let wait = if boundary > current_mins {
                    boundary - current_mins
                } else {
                    24 * 60 - current_mins + boundary
                };

                if wait > 0 {
                    min_wait = Some(min_wait.map_or(wait, |m| m.min(wait)));
                }
            }
        }

        min_wait.map(|mins| Duration::from_secs((mins as u64) * 60))
    }

    /// Load schedule from JSON file
    pub fn load<P: AsRef<std::path::Path>>(path: P) -> std::io::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        serde_json::from_str(&content)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }

    /// Save schedule to JSON file
    pub fn save<P: AsRef<std::path::Path>>(&self, path: P) -> std::io::Result<()> {
        let content = serde_json::to_string_pretty(self)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        std::fs::write(path, content)
    }
}

impl Default for BandwidthSchedule {
    fn default() -> Self {
        Self::new()
    }
}

/// Scheduled bandwidth limiter
pub struct ScheduledLimiter {
    schedule: BandwidthSchedule,
    current_limit: AtomicU64,
    tokens: AtomicU64,
    last_update: std::sync::Mutex<std::time::Instant>,
    running: AtomicBool,
}

impl ScheduledLimiter {
    /// Create a new scheduled limiter
    pub fn new(schedule: BandwidthSchedule) -> Arc<Self> {
        let current_limit = schedule.current_limit();

        Arc::new(Self {
            schedule,
            current_limit: AtomicU64::new(current_limit),
            tokens: AtomicU64::new(current_limit),
            last_update: std::sync::Mutex::new(std::time::Instant::now()),
            running: AtomicBool::new(true),
        })
    }

    /// Start background schedule updater
    pub fn start_updater(self: &Arc<Self>) -> std::thread::JoinHandle<()> {
        let limiter = Arc::clone(self);

        std::thread::spawn(move || {
            while limiter.running.load(Ordering::Relaxed) {
                let new_limit = limiter.schedule.current_limit();
                limiter.current_limit.store(new_limit, Ordering::Relaxed);

                // Sleep until next schedule change or 1 minute
                let sleep_duration = limiter.schedule.next_change()
                    .unwrap_or(Duration::from_secs(60))
                    .min(Duration::from_secs(60));

                std::thread::sleep(sleep_duration);
            }
        })
    }

    /// Stop the updater
    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
    }

    /// Acquire tokens for transfer
    pub fn acquire(&self, bytes: u64) -> Duration {
        let limit = self.current_limit.load(Ordering::Relaxed);

        if limit == 0 {
            return Duration::ZERO; // Unlimited
        }

        // Token bucket algorithm
        let mut last_update = self.last_update.lock().unwrap();
        let now = std::time::Instant::now();
        let elapsed = now.duration_since(*last_update);
        *last_update = now;
        drop(last_update);

        // Add tokens based on elapsed time using atomic loop to avoid race conditions
        let new_tokens = (elapsed.as_secs_f64() * limit as f64) as u64;
        let cap = limit * 2; // Cap at 2 seconds worth
        let total_tokens = loop {
            let current = self.tokens.load(Ordering::Relaxed);
            let desired = (current + new_tokens).min(cap);
            match self.tokens.compare_exchange_weak(current, desired, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break desired,
                Err(_) => continue, // Retry on contention
            }
        };

        if total_tokens >= bytes {
            self.tokens.fetch_sub(bytes, Ordering::Relaxed);
            Duration::ZERO
        } else {
            // Need to wait
            let needed = bytes - total_tokens;
            let wait_secs = needed as f64 / limit as f64;
            Duration::from_secs_f64(wait_secs)
        }
    }

    /// Get current limit
    pub fn current_limit(&self) -> u64 {
        self.current_limit.load(Ordering::Relaxed)
    }

    /// Check if currently limited
    pub fn is_limited(&self) -> bool {
        self.current_limit.load(Ordering::Relaxed) > 0
    }

    /// Get schedule info for display
    pub fn status(&self) -> ScheduleStatus {
        let limit = self.current_limit.load(Ordering::Relaxed);
        let next_change = self.schedule.next_change();

        ScheduleStatus {
            enabled: self.schedule.enabled,
            current_limit: limit,
            is_limited: limit > 0,
            next_change,
            active_rule: self.get_active_rule(),
        }
    }

    fn get_active_rule(&self) -> Option<String> {
        if !self.schedule.enabled {
            return None;
        }

        let now = Local::now();
        let current_day = DayOfWeek::from_chrono(now.weekday());
        let current_time = TimeOfDay::new(now.hour(), now.minute());

        for rule in &self.schedule.rules {
            if self.schedule.rule_matches(rule, current_day, current_time) {
                return Some(rule.name.clone());
            }
        }

        None
    }
}

/// Status of the bandwidth schedule
#[derive(Debug, Clone)]
pub struct ScheduleStatus {
    /// Whether scheduling is enabled
    pub enabled: bool,
    /// Current bandwidth limit (bytes/sec)
    pub current_limit: u64,
    /// Whether transfers are currently limited
    pub is_limited: bool,
    /// Time until next schedule change
    pub next_change: Option<Duration>,
    /// Name of currently active rule
    pub active_rule: Option<String>,
}

impl ScheduleStatus {
    /// Format limit for display
    pub fn limit_display(&self) -> String {
        if self.current_limit == 0 {
            "Unlimited".to_string()
        } else {
            format_bandwidth(self.current_limit)
        }
    }
}

/// Format bandwidth for human display
pub fn format_bandwidth(bytes_per_sec: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;

    if bytes_per_sec >= GB {
        format!("{:.2} GB/s", bytes_per_sec as f64 / GB as f64)
    } else if bytes_per_sec >= MB {
        format!("{:.2} MB/s", bytes_per_sec as f64 / MB as f64)
    } else if bytes_per_sec >= KB {
        format!("{:.2} KB/s", bytes_per_sec as f64 / KB as f64)
    } else {
        format!("{} B/s", bytes_per_sec)
    }
}

/// Parse bandwidth string (e.g., "100M", "1G", "500K")
pub fn parse_bandwidth(s: &str) -> Option<u64> {
    let s = s.trim().to_uppercase();

    let (num_str, multiplier) = if s.ends_with("G") || s.ends_with("GB") {
        (s.trim_end_matches("GB").trim_end_matches('G'), 1024 * 1024 * 1024)
    } else if s.ends_with("M") || s.ends_with("MB") {
        (s.trim_end_matches("MB").trim_end_matches('M'), 1024 * 1024)
    } else if s.ends_with("K") || s.ends_with("KB") {
        (s.trim_end_matches("KB").trim_end_matches('K'), 1024)
    } else {
        (s.as_str(), 1)
    };

    num_str.trim().parse::<f64>().ok().map(|n| (n * multiplier as f64) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_time_of_day() {
        let t = TimeOfDay::from_hhmm("09:30").unwrap();
        assert_eq!(t.hour, 9);
        assert_eq!(t.minute, 30);
        assert_eq!(t.to_minutes(), 570);
    }

    #[test]
    fn test_parse_bandwidth() {
        assert_eq!(parse_bandwidth("100M"), Some(104_857_600));
        assert_eq!(parse_bandwidth("1G"), Some(1_073_741_824));
        assert_eq!(parse_bandwidth("500K"), Some(512_000));
        assert_eq!(parse_bandwidth("1024"), Some(1024));
    }

    #[test]
    fn test_business_hours_schedule() {
        let schedule = BandwidthSchedule::business_hours(
            100 * 1024 * 1024,  // 100 MB/s during day
            0,                   // Unlimited at night
        );

        assert_eq!(schedule.rules.len(), 2);
        assert!(schedule.enabled);
    }

    #[test]
    fn test_schedule_serialization() {
        let schedule = BandwidthSchedule::business_hours(100_000_000, 0);
        let json = serde_json::to_string(&schedule).unwrap();
        let loaded: BandwidthSchedule = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.rules.len(), schedule.rules.len());
    }
}
