mod app;
mod config;
mod ipc;
mod kafka;
mod period;
mod symbol;
mod proto;

use anyhow::{ensure, Result};
use clap::Parser;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Path to TOML config file.
    #[arg(long, default_value = "config.toml")]
    config: String,

    /// List Kafka topics and exit.
    #[arg(long)]
    list_topics: bool,

    /// Force inc snapshots for testing.
    #[arg(long)]
    test: bool,
}

fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = Args::parse();
    let cfg = config::Config::load(&args.config)?;
    if args.list_topics {
        let topics = kafka::list_topics(&cfg.kafka_brokers, Duration::from_secs(5))?;
        for topic in topics {
            println!("{}", topic);
        }
        return Ok(());
    }
    ensure!(
        cfg.tick_interval_ms > 0 && 3_000 % cfg.tick_interval_ms == 0,
        "tick_interval_ms must be > 0 and divide 3000"
    );
    ensure!(cfg.open_delay_us >= 0, "open_delay_us must be >= 0");
    ensure!(cfg.hedge_delay_us >= 0, "hedge_delay_us must be >= 0");
    ensure!(cfg.tick_delay_us >= 0, "tick_delay_us must be >= 0");
    let online_symbols = cfg.online_symbol_set();
    let mut app = app::App::new(
        &cfg.ipc_dir,
        online_symbols,
        cfg.tick_interval_ms,
        cfg.open_delay_us,
        cfg.hedge_delay_us,
        cfg.tick_delay_us,
        args.test,
    )?;

    let open_topic = kafka::topic_for_venue(&cfg.open_venue)?;
    let hedge_topic = kafka::topic_for_venue(&cfg.hedge_venue)?;
    let mut topics = vec![open_topic.clone(), hedge_topic.clone()];
    topics.sort();
    topics.dedup();
    let group_id = if cfg.kafka_group_unique {
        let ts_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        format!("{}-{}", cfg.kafka_group, ts_ms)
    } else {
        cfg.kafka_group.clone()
    };
    let source = kafka::KafkaSource::new(
        &cfg.kafka_brokers,
        &group_id,
        &topics,
        &cfg.kafka_offset_reset,
    )?;
    let warn_interval = Duration::from_secs(cfg.period_warn_interval_secs);
    let mut aligner = period::PeriodAligner::new(warn_interval);

    loop {
        if let Some(record) = source.poll(Duration::from_millis(200))? {
            let is_open = if record.topic == open_topic {
                true
            } else if record.topic == hedge_topic {
                false
            } else {
                continue;
            };

            let msg = app::decode_period_message(&record.payload)?;
            let pair = if is_open {
                aligner.insert_open(msg)
            } else {
                aligner.insert_hedge(msg)
            };

            if let Some((open_msg, hedge_msg)) = pair {
                app.handle_pair(&cfg.open_venue, &cfg.hedge_venue, open_msg, hedge_msg)?;
            }

            aligner.warn_if_gap(&cfg.open_venue, &cfg.hedge_venue);
            aligner.log_status(&cfg.open_venue, &cfg.hedge_venue);
        }
    }
}
