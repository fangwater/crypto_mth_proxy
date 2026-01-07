use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashSet;
use std::fs;
use std::path::Path;

use crate::symbol;

fn default_kafka_brokers() -> String {
    "10.61.10.22:9092".to_string()
}

fn default_kafka_group() -> String {
    "crypto_mth_proxy".to_string()
}

fn default_kafka_offset_reset() -> String {
    "latest".to_string()
}

fn default_kafka_group_unique() -> bool {
    false
}

fn default_ipc_dir() -> String {
    "/tmp/mth_pubs".to_string()
}

fn default_open_venue() -> String {
    "okex-futures".to_string()
}

fn default_hedge_venue() -> String {
    "binance-futures".to_string()
}

fn default_period_warn_interval_secs() -> u64 {
    5
}

fn default_tick_interval_ms() -> i64 {
    1_000
}

fn default_open_delay_us() -> i64 {
    0
}

fn default_hedge_delay_us() -> i64 {
    0
}

fn default_tick_delay_us() -> i64 {
    0
}

#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(default = "default_open_venue")]
    pub open_venue: String,

    #[serde(default = "default_hedge_venue")]
    pub hedge_venue: String,

    #[serde(default = "default_kafka_brokers")]
    pub kafka_brokers: String,

    #[serde(default = "default_kafka_group")]
    pub kafka_group: String,

    #[serde(default = "default_kafka_group_unique")]
    pub kafka_group_unique: bool,

    #[serde(default = "default_kafka_offset_reset")]
    pub kafka_offset_reset: String,

    #[serde(default = "default_ipc_dir")]
    pub ipc_dir: String,

    #[serde(default = "default_period_warn_interval_secs")]
    pub period_warn_interval_secs: u64,

    #[serde(default = "default_tick_interval_ms")]
    pub tick_interval_ms: i64,

    #[serde(default = "default_open_delay_us")]
    pub open_delay_us: i64,

    #[serde(default = "default_hedge_delay_us")]
    pub hedge_delay_us: i64,

    #[serde(default = "default_tick_delay_us")]
    pub tick_delay_us: i64,

    #[serde(default)]
    pub online_symbols: Vec<String>,
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let raw = fs::read_to_string(path)
            .with_context(|| format!("failed to read config {}", path.display()))?;
        let cfg: Config = toml::from_str(&raw)
            .with_context(|| format!("failed to parse toml {}", path.display()))?;
        Ok(cfg)
    }

    pub fn online_symbol_set(&self) -> HashSet<String> {
        let set = self
            .online_symbols
            .iter()
            .map(|s| symbol::normalize_for_whitelist(s))
            .filter(|s| !s.is_empty())
            .collect::<HashSet<_>>();
        set
    }
}
