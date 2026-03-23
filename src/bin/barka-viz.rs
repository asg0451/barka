use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use barka::leader_election;
use barka::node;
use barka::partition_registry::PartitionRegistry;
use barka::s3;
use clap::Parser;
use crossterm::event::{self, Event, KeyCode, KeyModifiers};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Color, Modifier, Style, Stylize};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};

#[derive(Parser)]
#[command(
    name = "barka-viz",
    version,
    about = "TUI cluster visualizer for barka"
)]
struct Cli {
    /// S3 endpoint URL (e.g. http://localhost:4566).
    #[arg(long)]
    s3_endpoint: Option<String>,

    /// S3 bucket.
    #[arg(long, default_value = "barka")]
    s3_bucket: String,

    /// AWS region for S3.
    #[arg(long, default_value = "us-east-1")]
    aws_region: String,

    /// S3 key prefix for leader election lock files.
    #[arg(long)]
    leader_election_prefix: Option<String>,

    /// S3 key prefix for data (segments).
    #[arg(long)]
    s3_prefix: Option<String>,

    /// Poll interval in seconds.
    #[arg(long, default_value_t = 2)]
    poll_secs: u64,
}

impl Cli {
    fn s3_config(&self) -> s3::S3Config {
        s3::S3Config {
            endpoint_url: self.s3_endpoint.clone(),
            bucket: self.s3_bucket.clone(),
            region: self.aws_region.clone(),
        }
    }
}

#[derive(Default)]
struct AppState {
    last_refresh: Option<Instant>,
    partitions: Vec<PartitionState>,
    total_topics: usize,
    total_partitions: usize,
    total_segments: usize,
    node_count: usize,
    last_error: Option<String>,
}

struct PartitionState {
    topic: String,
    partition: u32,
    leader: Option<LeaderState>,
    segment_count: usize,
}

struct LeaderState {
    node_id: u64,
    addr: SocketAddr,
    epoch: u64,
    valid_until_ms: u64,
}

async fn refresh_from_s3(
    s3_client: &aws_sdk_s3::Client,
    bucket: &str,
    le_prefix: Option<&str>,
    s3_prefix: Option<&str>,
) -> Result<AppState> {
    let registry = PartitionRegistry::new(s3_client.clone(), bucket.to_string(), le_prefix);
    let mut entries = registry.list().await?;
    entries.sort_by(|a, b| a.topic.cmp(&b.topic).then(a.partition.cmp(&b.partition)));

    let base_prefix = node::segment_key_prefix(s3_prefix);
    let mut partitions = Vec::with_capacity(entries.len());
    let mut total_segments = 0usize;
    let mut node_ids = HashSet::new();

    for entry in &entries {
        let namespace = node::leader_namespace(&entry.topic, entry.partition);
        let leader = leader_election::read_current_leader(s3_client, bucket, &namespace, le_prefix)
            .await
            .ok()
            .flatten();

        let data_prefix = node::partition_data_prefix(&base_prefix, &entry.topic, entry.partition);
        let seg_count = s3::list_objects(s3_client, bucket, &data_prefix)
            .await
            .map(|objs| objs.len())
            .unwrap_or(0);
        total_segments += seg_count;

        if let Some(ref l) = leader {
            node_ids.insert(l.node_id);
        }

        partitions.push(PartitionState {
            topic: entry.topic.clone(),
            partition: entry.partition,
            leader: leader.map(|l| LeaderState {
                node_id: l.node_id,
                addr: l.addr,
                epoch: l.epoch.as_u64(),
                valid_until_ms: l.valid_until_ms,
            }),
            segment_count: seg_count,
        });
    }

    let unique_topics: HashSet<&str> = entries.iter().map(|e| e.topic.as_str()).collect();

    Ok(AppState {
        last_refresh: Some(Instant::now()),
        total_topics: unique_topics.len(),
        total_partitions: partitions.len(),
        total_segments,
        node_count: node_ids.len(),
        partitions,
        last_error: None,
    })
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn format_ttl(valid_until_ms: u64) -> (String, Style) {
    let now = now_ms();
    if valid_until_ms <= now {
        return ("expired".into(), Style::default().fg(Color::Red));
    }
    let remaining_s = (valid_until_ms - now) / 1000;
    let style = if remaining_s < 3 {
        Style::default().fg(Color::Yellow)
    } else {
        Style::default().fg(Color::Green)
    };
    (format!("{remaining_s}s"), style)
}

fn render(frame: &mut Frame, state: &AppState) {
    let chunks = Layout::vertical([
        Constraint::Length(3),
        Constraint::Min(5),
        Constraint::Length(1),
    ])
    .split(frame.area());

    render_header(frame, chunks[0], state);
    render_table(frame, chunks[1], state);
    render_footer(frame, chunks[2], state);
}

fn render_header(frame: &mut Frame, area: ratatui::layout::Rect, state: &AppState) {
    let refresh_ago = state
        .last_refresh
        .map(|t| {
            let secs = t.elapsed().as_secs();
            format!("{secs}s ago")
        })
        .unwrap_or_else(|| "...".into());

    let line = Line::from(vec![
        Span::styled("Barka Cluster", Style::default().bold()),
        Span::raw("  "),
        Span::raw(format!("{} topics", state.total_topics)),
        Span::styled(" · ", Style::default().fg(Color::DarkGray)),
        Span::raw(format!("{} partitions", state.total_partitions)),
        Span::styled(" · ", Style::default().fg(Color::DarkGray)),
        Span::raw(format!("{} nodes", state.node_count)),
        Span::styled(" · ", Style::default().fg(Color::DarkGray)),
        Span::raw(format!("{} segments", state.total_segments)),
        Span::raw("  "),
        Span::styled(
            format!("[{refresh_ago}]"),
            Style::default().fg(Color::DarkGray),
        ),
    ]);

    let block = Block::default().borders(Borders::BOTTOM);
    let paragraph = Paragraph::new(line).block(block);
    frame.render_widget(paragraph, area);
}

fn render_table(frame: &mut Frame, area: ratatui::layout::Rect, state: &AppState) {
    let header = Row::new([
        Cell::from("Topic"),
        Cell::from("Part"),
        Cell::from("Leader"),
        Cell::from("Addr"),
        Cell::from("Epoch"),
        Cell::from("TTL"),
        Cell::from("Segs"),
    ])
    .style(
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    );

    let dim = Style::default().fg(Color::DarkGray);

    let rows: Vec<Row> = if state.partitions.is_empty() {
        vec![Row::new([
            Cell::from("No partitions registered.").style(dim),
            Cell::default(),
            Cell::default(),
            Cell::default(),
            Cell::default(),
            Cell::default(),
            Cell::default(),
        ])]
    } else {
        state
            .partitions
            .iter()
            .map(|p| {
                let (leader, addr, epoch, ttl_cell) = match &p.leader {
                    Some(l) => {
                        let (ttl_str, ttl_style) = format_ttl(l.valid_until_ms);
                        (
                            Cell::from(l.node_id.to_string()),
                            Cell::from(l.addr.to_string()),
                            Cell::from(l.epoch.to_string()),
                            Cell::from(ttl_str).style(ttl_style),
                        )
                    }
                    None => (
                        Cell::from("---").style(dim),
                        Cell::from("---").style(dim),
                        Cell::from("---").style(dim),
                        Cell::from("---").style(dim),
                    ),
                };
                Row::new([
                    Cell::from(p.topic.as_str()),
                    Cell::from(p.partition.to_string()),
                    leader,
                    addr,
                    epoch,
                    ttl_cell,
                    Cell::from(p.segment_count.to_string()),
                ])
            })
            .collect()
    };

    let widths = [
        Constraint::Min(12),
        Constraint::Length(6),
        Constraint::Length(8),
        Constraint::Min(18),
        Constraint::Length(7),
        Constraint::Length(8),
        Constraint::Length(6),
    ];

    let table = Table::new(rows, widths)
        .header(header)
        .row_highlight_style(Style::default());
    frame.render_widget(table, area);
}

fn render_footer(frame: &mut Frame, area: ratatui::layout::Rect, state: &AppState) {
    let mut spans = vec![
        Span::styled("q", Style::default().bold()),
        Span::raw(": quit  "),
        Span::styled("r", Style::default().bold()),
        Span::raw(": refresh now"),
    ];

    if let Some(ref err) = state.last_error {
        spans.push(Span::raw("  "));
        spans.push(Span::styled(
            format!("err: {err}"),
            Style::default().fg(Color::Red),
        ));
    }

    let footer = Paragraph::new(Line::from(spans));
    frame.render_widget(footer, area);
}

async fn run_app(
    terminal: &mut ratatui::DefaultTerminal,
    s3_client: &aws_sdk_s3::Client,
    bucket: &str,
    le_prefix: Option<&str>,
    s3_prefix: Option<&str>,
    poll_interval: Duration,
) -> Result<()> {
    let tick_rate = Duration::from_millis(100);
    let mut state = AppState::default();
    let mut last_poll = Instant::now() - poll_interval;

    loop {
        if last_poll.elapsed() >= poll_interval {
            match refresh_from_s3(s3_client, bucket, le_prefix, s3_prefix).await {
                Ok(new_state) => state = new_state,
                Err(e) => {
                    state.last_error = Some(format!("{e:#}"));
                    state.last_refresh = Some(Instant::now());
                }
            }
            last_poll = Instant::now();
        }

        terminal.draw(|frame| render(frame, &state))?;

        if event::poll(tick_rate)?
            && let Event::Key(key) = event::read()?
        {
            match key.code {
                KeyCode::Char('q') | KeyCode::Esc => return Ok(()),
                KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    return Ok(());
                }
                KeyCode::Char('r') => {
                    last_poll = Instant::now() - poll_interval;
                }
                _ => {}
            }
        }
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let s3_config = cli.s3_config();
    let poll_interval = Duration::from_secs(cli.poll_secs);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("tokio runtime")?;

    let mut terminal = ratatui::init();
    let result = rt.block_on(async {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let s3_client = s3::build_client(&s3_config).await;
                run_app(
                    &mut terminal,
                    &s3_client,
                    &s3_config.bucket,
                    cli.leader_election_prefix.as_deref(),
                    cli.s3_prefix.as_deref(),
                    poll_interval,
                )
                .await
            })
            .await
    });

    ratatui::restore();
    result
}
