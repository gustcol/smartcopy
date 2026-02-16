//! Terminal User Interface (TUI) dashboard for real-time transfer monitoring
//!
//! Provides a rich terminal dashboard using Ratatui with:
//! - Overall progress gauge
//! - Per-file transfer table
//! - Throughput sparkline
//! - Key bindings: q=quit, p=pause, s=sort

use std::io;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind},
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
    ExecutableCommand,
};
use ratatui::{
    prelude::*,
    widgets::{Block, Borders, Gauge, Paragraph, Row, Sparkline, Table},
};

/// Transfer statistics shared between the TUI and the copy engine.
#[derive(Debug, Clone, Default)]
pub struct TransferStats {
    pub total_files: u64,
    pub files_done: u64,
    pub total_bytes: u64,
    pub bytes_done: u64,
    pub current_file: String,
    pub throughput_mbps: f64,
    pub throughput_history: Vec<u64>,
    pub elapsed: Duration,
    pub is_paused: bool,
    pub errors: usize,
}

impl TransferStats {
    pub fn progress_pct(&self) -> f64 {
        if self.total_bytes == 0 {
            0.0
        } else {
            (self.bytes_done as f64 / self.total_bytes as f64) * 100.0
        }
    }

    pub fn eta(&self) -> Option<Duration> {
        if self.throughput_mbps <= 0.0 || self.bytes_done == 0 {
            return None;
        }
        let remaining = self.total_bytes.saturating_sub(self.bytes_done) as f64;
        let bytes_per_sec = self.throughput_mbps * 1024.0 * 1024.0;
        if bytes_per_sec > 0.0 {
            Some(Duration::from_secs_f64(remaining / bytes_per_sec))
        } else {
            None
        }
    }
}

/// Shared state for the TUI.
pub type SharedStats = Arc<Mutex<TransferStats>>;

/// The TUI dashboard controller.
pub struct TuiDashboard {
    stats: SharedStats,
}

impl TuiDashboard {
    pub fn new() -> Self {
        Self {
            stats: Arc::new(Mutex::new(TransferStats::default())),
        }
    }

    /// Get a clone of the shared stats handle for updating from the copy engine.
    pub fn stats_handle(&self) -> SharedStats {
        Arc::clone(&self.stats)
    }

    /// Spawn the TUI in a background thread. Returns a join handle.
    pub fn spawn(self) -> thread::JoinHandle<()> {
        let stats = self.stats;
        thread::spawn(move || {
            if let Err(e) = run_tui(stats) {
                eprintln!("TUI error: {}", e);
            }
        })
    }
}

impl Default for TuiDashboard {
    fn default() -> Self {
        Self::new()
    }
}

fn run_tui(stats: SharedStats) -> io::Result<()> {
    enable_raw_mode()?;
    io::stdout().execute(EnterAlternateScreen)?;
    let mut terminal = Terminal::new(CrosstermBackend::new(io::stdout()))?;

    let tick_rate = Duration::from_millis(250);
    let mut last_tick = Instant::now();

    loop {
        terminal.draw(|frame| draw_ui(frame, &stats))?;

        let timeout = tick_rate.saturating_sub(last_tick.elapsed());
        if event::poll(timeout)? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    match key.code {
                        KeyCode::Char('q') | KeyCode::Esc => {
                            break;
                        }
                        KeyCode::Char('p') => {
                            if let Ok(mut s) = stats.lock() {
                                s.is_paused = !s.is_paused;
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        if last_tick.elapsed() >= tick_rate {
            last_tick = Instant::now();
        }
    }

    disable_raw_mode()?;
    io::stdout().execute(LeaveAlternateScreen)?;
    Ok(())
}

fn draw_ui(frame: &mut Frame, stats: &SharedStats) {
    let s = stats.lock().unwrap().clone();

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .margin(1)
        .constraints([
            Constraint::Length(3), // Title
            Constraint::Length(3), // Progress gauge
            Constraint::Length(5), // Stats
            Constraint::Min(5),   // Throughput sparkline
            Constraint::Length(2), // Footer
        ])
        .split(frame.area());

    // Title
    let title = Paragraph::new("SmartCopy Transfer Dashboard")
        .style(Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD))
        .alignment(Alignment::Center)
        .block(Block::default().borders(Borders::BOTTOM));
    frame.render_widget(title, chunks[0]);

    // Progress gauge
    let pct = s.progress_pct();
    let label = format!(
        "{:.1}% ({}/{} files, {}/{})",
        pct,
        s.files_done,
        s.total_files,
        format_bytes(s.bytes_done),
        format_bytes(s.total_bytes),
    );
    let gauge = Gauge::default()
        .block(Block::default().title(" Progress ").borders(Borders::ALL))
        .gauge_style(
            Style::default()
                .fg(if s.is_paused { Color::Yellow } else { Color::Green })
                .bg(Color::DarkGray),
        )
        .percent(pct.min(100.0) as u16)
        .label(label);
    frame.render_widget(gauge, chunks[1]);

    // Stats table
    let eta_str = s
        .eta()
        .map(|d| format!("{}s", d.as_secs()))
        .unwrap_or_else(|| "---".to_string());
    let status = if s.is_paused { "PAUSED" } else { "ACTIVE" };

    let rows = vec![
        Row::new(vec![
            format!("Throughput: {:.1} MB/s", s.throughput_mbps),
            format!("Elapsed: {}s", s.elapsed.as_secs()),
            format!("ETA: {}", eta_str),
            format!("Status: {}", status),
            format!("Errors: {}", s.errors),
        ]),
        Row::new(vec![
            format!("Current: {}", truncate_path(&s.current_file, 60)),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
        ]),
    ];

    let widths = [
        Constraint::Percentage(30),
        Constraint::Percentage(20),
        Constraint::Percentage(15),
        Constraint::Percentage(15),
        Constraint::Percentage(20),
    ];

    let table = Table::new(rows, widths)
        .block(Block::default().title(" Statistics ").borders(Borders::ALL));
    frame.render_widget(table, chunks[2]);

    // Throughput sparkline
    let sparkline_data: Vec<u64> = if s.throughput_history.len() > 60 {
        s.throughput_history[s.throughput_history.len() - 60..].to_vec()
    } else {
        s.throughput_history.clone()
    };

    let sparkline = Sparkline::default()
        .block(
            Block::default()
                .title(" Throughput (MB/s) ")
                .borders(Borders::ALL),
        )
        .data(&sparkline_data)
        .style(Style::default().fg(Color::Cyan));
    frame.render_widget(sparkline, chunks[3]);

    // Footer
    let footer = Paragraph::new(" q: quit | p: pause/resume")
        .style(Style::default().fg(Color::DarkGray))
        .alignment(Alignment::Center);
    frame.render_widget(footer, chunks[4]);
}

fn format_bytes(bytes: u64) -> String {
    if bytes >= 1024 * 1024 * 1024 {
        format!("{:.1} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    } else if bytes >= 1024 * 1024 {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{} B", bytes)
    }
}

fn truncate_path(path: &str, max_len: usize) -> String {
    if path.len() <= max_len {
        path.to_string()
    } else {
        format!("...{}", &path[path.len() - max_len + 3..])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transfer_stats_progress() {
        let mut stats = TransferStats::default();
        assert_eq!(stats.progress_pct(), 0.0);

        stats.total_bytes = 1000;
        stats.bytes_done = 500;
        assert!((stats.progress_pct() - 50.0).abs() < 0.01);
    }

    #[test]
    fn test_transfer_stats_eta() {
        let mut stats = TransferStats::default();
        assert!(stats.eta().is_none());

        stats.total_bytes = 200 * 1024 * 1024;
        stats.bytes_done = 100 * 1024 * 1024;
        stats.throughput_mbps = 100.0;
        let eta = stats.eta().unwrap();
        assert!(eta.as_secs() > 0);
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(500), "500 B");
        assert_eq!(format_bytes(1500), "1.5 KB");
        assert_eq!(format_bytes(1_500_000), "1.4 MB");
        assert_eq!(format_bytes(1_500_000_000), "1.4 GB");
    }

    #[test]
    fn test_truncate_path() {
        assert_eq!(truncate_path("short", 10), "short");
        let long = "/very/long/path/to/some/deeply/nested/file.txt";
        let truncated = truncate_path(long, 20);
        assert!(truncated.starts_with("..."));
        assert!(truncated.len() <= 20);
    }
}
