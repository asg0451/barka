//! Decode barka binary segment files to JSON (see `segment` module layout).

use std::fs;
use std::io::{self, Read};
use std::path::PathBuf;

use anyhow::Context;
use base64::{engine::general_purpose::STANDARD as B64, Engine};
use barka::segment;
use clap::Parser;
use serde::Serialize;

#[derive(Parser)]
#[command(name = "barka-segment", version, about = "Parse barka segment files to JSON")]
struct Cli {
    /// Input segment file, or `-` for stdin (default).
    #[arg(value_name = "FILE", default_value = "-")]
    input: PathBuf,

    /// Single-line JSON (default: pretty-printed).
    #[arg(long)]
    compact: bool,
}

#[derive(Serialize)]
struct JsonRecord {
    offset: u64,
    timestamp: i64,
    /// Present when key bytes are valid UTF-8 (including empty).
    #[serde(skip_serializing_if = "Option::is_none")]
    key: Option<String>,
    /// Present when key bytes are not valid UTF-8.
    #[serde(skip_serializing_if = "Option::is_none")]
    key_base64: Option<String>,
    /// Present when value bytes are valid UTF-8.
    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<String>,
    /// Present when value bytes are not valid UTF-8.
    #[serde(skip_serializing_if = "Option::is_none")]
    value_base64: Option<String>,
}

fn utf8_or_base64(bytes: &[u8]) -> (Option<String>, Option<String>) {
    match std::str::from_utf8(bytes) {
        Ok(s) => (Some(s.to_string()), None),
        Err(_) => (None, Some(B64.encode(bytes))),
    }
}

#[derive(Serialize)]
struct JsonSegment {
    epoch: u64,
    records: Vec<JsonRecord>,
}

fn read_input(path: &PathBuf) -> anyhow::Result<Vec<u8>> {
    if path.as_os_str() == "-" {
        let mut buf = Vec::new();
        io::stdin()
            .read_to_end(&mut buf)
            .context("read stdin")?;
        Ok(buf)
    } else {
        fs::read(path).with_context(|| format!("read {}", path.display()))
    }
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let raw = read_input(&cli.input)?;
    let (epoch, records) = segment::decode(&raw)?;

    let json_records: Vec<JsonRecord> = records
        .into_iter()
        .map(|r| {
            let (key, key_base64) = utf8_or_base64(&r.key);
            let (value, value_base64) = utf8_or_base64(&r.value);
            JsonRecord {
                offset: r.offset,
                timestamp: r.timestamp,
                key,
                key_base64,
                value,
                value_base64,
            }
        })
        .collect();

    let out = JsonSegment {
        epoch,
        records: json_records,
    };

    let json = if cli.compact {
        serde_json::to_string(&out)?
    } else {
        serde_json::to_string_pretty(&out)?
    };
    println!("{json}");
    Ok(())
}
