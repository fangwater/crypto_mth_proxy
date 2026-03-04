use anyhow::{anyhow, Context, Result};
use flate2::read::ZlibDecoder;
use log::{debug, info};
use prost::Message;
use std::io::Read;

use crate::ipc::{AssetType, PublisherManager};
use crate::proto::{PeriodMessage, SymbolInfo};
use crate::symbol;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::ops::DerefMut;
use zmq::Message as ZmqMessage;

const EVT_INC: u8 = 1;
const EVT_TRADE: u8 = 2;
const EVT_TICK: u8 = 3;
const PERIOD_MS: i64 = 3_000;
const FOCUS_SYMBOLS: [&str; 2] = ["BTCUSDT", "ETHUSDT"];

pub struct App {
    publishers: PublisherManager,
    online_symbols: HashSet<String>,
    tick_interval_ms: i64,
    open_delay_us: i64,
    hedge_delay_us: i64,
    tick_delay_us: i64,
    force_snapshot: bool,
    dump_writer: Option<BufWriter<File>>,
    dump_symbol: Option<String>,
    dump_exit: bool,
    dump_period_limit: Option<usize>,
    dump_periods: usize,
    dumped: bool,
}

impl App {
    pub fn new(
        ipc_dir: &str,
        online_symbols: HashSet<String>,
        tick_interval_ms: i64,
        open_delay_us: i64,
        hedge_delay_us: i64,
        tick_delay_us: i64,
        force_snapshot: bool,
    ) -> Result<Self> {
        let publishers = PublisherManager::new(ipc_dir)?;
        let dump_writer = match std::env::var("DUMP_PATH") {
            Ok(path) if !path.trim().is_empty() => {
                let file = File::create(path.trim())?;
                Some(BufWriter::new(file))
            }
            _ => None,
        };
        let dump_symbol = std::env::var("DUMP_SYMBOL")
            .ok()
            .map(|value| symbol::normalize_for_whitelist(&value))
            .filter(|value| !value.is_empty());
        let dump_exit = std::env::var("DUMP_EXIT")
            .ok()
            .map(|value| value != "0")
            .unwrap_or(false);
        let dump_period_limit = std::env::var("DUMP_PERIODS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0);
        Ok(Self {
            publishers,
            online_symbols,
            tick_interval_ms,
            open_delay_us,
            hedge_delay_us,
            tick_delay_us,
            force_snapshot,
            dump_writer,
            dump_symbol,
            dump_exit,
            dump_period_limit,
            dump_periods: 0,
            dumped: false,
        })
    }

    pub fn handle_pair(
        &mut self,
        open_venue: &str,
        hedge_venue: &str,
        open_msg: PeriodMessage,
        hedge_msg: PeriodMessage,
    ) -> Result<()> {
        log_period_stats(open_venue, &open_msg);
        log_period_stats(hedge_venue, &hedge_msg);

        let channel = format!("{}-{}", open_venue, hedge_venue);
        let mut buckets: HashMap<String, SymbolBucket> = HashMap::new();

        let period_start_ms = open_msg.ts.saturating_sub(PERIOD_MS);
        let period_start_us = period_start_ms
            .saturating_mul(1_000)
            .saturating_add(self.tick_delay_us);
        info!(
            "apply delay channel={} open_delay_us={} hedge_delay_us={} tick_delay_us={} tick_start_us={}",
            channel,
            self.open_delay_us,
            self.hedge_delay_us,
            self.tick_delay_us,
            period_start_us
        );
        self.ingest_message(open_venue, Side::Open, open_msg, &mut buckets);
        self.ingest_message(hedge_venue, Side::Hedge, hedge_msg, &mut buckets);

        let tick_times = self.tick_times_us(period_start_us);
        let single_venue_mode = open_venue == hedge_venue;
        self.log_focus_symbol_stats(
            &channel,
            open_venue,
            hedge_venue,
            single_venue_mode,
            tick_times.len(),
            &buckets,
        );

        for (symbol, mut bucket) in buckets {
            if bucket.events.is_empty() {
                continue;
            }

            for ts_us in &tick_times {
                bucket.events.push(Event::tick(*ts_us));
            }

            bucket
                .events
                .sort_by_key(|event| (event.timestamp(), event.sort_rank()));

            if self.should_dump_symbol(&symbol) {
                self.dump_symbol_events(&channel, open_venue, hedge_venue, &symbol, &bucket.events);
                self.dumped = true;
                if let Some(limit) = self.dump_period_limit {
                    self.dump_periods += 1;
                    if self.dump_periods >= limit {
                        info!("dump periods reached {}, exiting", limit);
                        if let Some(writer) = self.dump_writer.as_mut() {
                            let _ = writer.flush();
                        }
                        std::process::exit(0);
                    }
                }
                if let Some(writer) = self.dump_writer.as_mut() {
                    let _ = writer.flush();
                }
                if self.dump_period_limit.is_none() && self.dump_exit {
                    info!("dump finished, exiting");
                    std::process::exit(0);
                }
            }
            for event in bucket.events {
                let msg = build_stream_message(&event);
                self.publishers
                    .publish(&channel, &symbol, AssetType::Stream, msg)?;
            }
        }

        Ok(())
    }

    fn ingest_message(
        &self,
        venue: &str,
        side: Side,
        msg: PeriodMessage,
        buckets: &mut HashMap<String, SymbolBucket>,
    ) {
        let (origin, delay_us) = match side {
            Side::Open => (Origin::Open, self.open_delay_us),
            Side::Hedge => (Origin::Hedge, self.hedge_delay_us),
        };
        for symbol_info in msg.symbol_infos {
            let SymbolInfo {
                symbol,
                trades,
                incs,
            } = symbol_info;
            let normalized = symbol::normalize_for_pairing(&symbol, venue);
            if normalized.is_empty() {
                continue;
            }
            if !self.online_symbols.contains(&normalized) {
                continue;
            }

            let bucket = buckets.entry(normalized).or_default();
            match side {
                Side::Open => {
                    bucket.open_trades += trades.len();
                    bucket.open_incs += incs.len();
                }
                Side::Hedge => {
                    bucket.hedge_trades += trades.len();
                    bucket.hedge_incs += incs.len();
                }
            }

            for trade in trades {
                if let Some(event) = Event::from_trade(trade, origin, delay_us) {
                    bucket.events.push(event);
                }
            }
            for inc in incs {
                bucket
                    .events
                    .extend(Event::from_inc(inc, origin, delay_us, self.force_snapshot));
            }
        }
    }

    fn log_focus_symbol_stats(
        &self,
        channel: &str,
        open_venue: &str,
        hedge_venue: &str,
        single_venue_mode: bool,
        ticks: usize,
        buckets: &HashMap<String, SymbolBucket>,
    ) {
        let mut details = Vec::with_capacity(FOCUS_SYMBOLS.len());
        for symbol in FOCUS_SYMBOLS {
            match buckets.get(symbol) {
                Some(bucket) => {
                    if single_venue_mode {
                        details.push(format!(
                            "{}{{open_trades={},open_incs={},hedge=NA,symbol_events={}}}",
                            symbol,
                            bucket.open_trades,
                            bucket.open_incs,
                            bucket.events.len()
                        ));
                    } else {
                        details.push(format!(
                            "{}{{open_trades={},open_incs={},hedge_trades={},hedge_incs={},symbol_events={}}}",
                            symbol,
                            bucket.open_trades,
                            bucket.open_incs,
                            bucket.hedge_trades,
                            bucket.hedge_incs,
                            bucket.events.len()
                        ));
                    }
                }
                None => details.push(format!("{}{{no_data}}", symbol)),
            }
        }

        info!(
            "focus stats channel={} open_venue={} hedge_venue={} mode={} ticks={} details=[{}]",
            channel,
            open_venue,
            hedge_venue,
            if single_venue_mode {
                "single_venue(open_only)"
            } else {
                "dual_venue"
            },
            ticks,
            details.join("; ")
        );
    }

    fn tick_times_us(&self, period_start_us: i64) -> Vec<i64> {
        let mut out = Vec::new();
        if self.tick_interval_ms <= 0 {
            return out;
        }
        let step_us = self.tick_interval_ms.saturating_mul(1_000);
        let period_us = PERIOD_MS.saturating_mul(1_000);
        let mut offset_us = 0i64;
        while offset_us < period_us {
            out.push(period_start_us.saturating_add(offset_us));
            offset_us += step_us;
        }
        out
    }

    fn should_dump_symbol(&self, symbol: &str) -> bool {
        let matches = match &self.dump_symbol {
            Some(target) => target == symbol,
            None => false,
        };
        if self.dump_period_limit.is_some() {
            return matches;
        }
        if self.dumped {
            return false;
        }
        matches
    }

    fn dump_symbol_events(
        &mut self,
        channel: &str,
        _open_venue: &str,
        _hedge_venue: &str,
        symbol: &str,
        events: &[Event],
    ) {
        if let Some(writer) = self.dump_writer.as_mut() {
            for event in events {
                let line = event.as_json_line(symbol);
                let _ = writer.write_all(line.as_bytes());
                let _ = writer.write_all(b"\n");
            }
            return;
        }

        info!(
            "dump symbol events channel={} symbol={} total={}",
            channel,
            symbol,
            events.len()
        );
        for (idx, event) in events.iter().enumerate() {
            info!(
                "dump event idx={} kind={} ts_us={} side_id={} is_snapshot={} price={} amount={} origin={}",
                idx,
                event.kind_label(),
                event.ts_us,
                event.side_id,
                event.is_snapshot,
                event.price,
                event.amount,
                event.origin as u8
            );
        }
    }
}

pub(crate) fn decode_period_message(bytes: &[u8]) -> Result<PeriodMessage> {
    let decompressed =
        decompress_zlib(bytes).with_context(|| "zlib decompress failed for PeriodMessage")?;
    debug!(
        "decoded zlib payload: {} -> {} bytes",
        bytes.len(),
        decompressed.len()
    );

    if let Ok(Some(msg)) = decode_with_length_prefix(&decompressed) {
        return Ok(msg);
    }

    PeriodMessage::decode(&*decompressed)
        .map_err(|err| anyhow!("failed to decode PeriodMessage after zlib: {err}"))
}

fn decode_with_length_prefix(bytes: &[u8]) -> Result<Option<PeriodMessage>> {
    if bytes.len() < 4 {
        return Ok(None);
    }
    let len = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    if len == 0 || len + 4 != bytes.len() {
        return Ok(None);
    }
    let payload = &bytes[4..];
    PeriodMessage::decode(payload)
        .map(Some)
        .map_err(|err| anyhow!("length-prefixed decode failed: {err}"))
}

fn decompress_zlib(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = ZlibDecoder::new(bytes);
    let mut out = Vec::new();
    decoder.read_to_end(&mut out)?;
    Ok(out)
}

#[derive(Clone, Copy)]
enum EventKind {
    Trade,
    Incremental,
    Tick,
}

#[derive(Default)]
struct SymbolBucket {
    events: Vec<Event>,
    open_trades: usize,
    open_incs: usize,
    hedge_trades: usize,
    hedge_incs: usize,
}

enum Side {
    Open,
    Hedge,
}

#[derive(Clone, Copy)]
enum Origin {
    Open = 0,
    Hedge = 1,
}

struct Event {
    kind: EventKind,
    ts_us: i64,
    side_id: u8,
    is_snapshot: u8,
    price: f64,
    amount: f64,
    origin: Origin,
}

impl Event {
    fn timestamp(&self) -> i64 {
        self.ts_us
    }

    fn kind_label(&self) -> &'static str {
        match self.kind {
            EventKind::Tick => "tick",
            EventKind::Incremental => "inc",
            EventKind::Trade => "trade",
        }
    }

    fn sort_rank(&self) -> u8 {
        match self.kind {
            EventKind::Tick => 0,
            EventKind::Incremental => 1,
            EventKind::Trade => 2,
        }
    }

    fn from_trade(trade: crate::proto::TradeInfo, origin: Origin, delay_us: i64) -> Option<Self> {
        let side_id = side_id_from_str(&trade.side)?;
        Some(Self {
            kind: EventKind::Trade,
            ts_us: normalize_ts_us(trade.timestamp).saturating_add(delay_us),
            side_id,
            is_snapshot: 0,
            price: trade.price,
            amount: trade.amount,
            origin,
        })
    }

    fn from_inc(
        inc: crate::proto::IncrementOrderBookInfo,
        origin: Origin,
        delay_us: i64,
        force_snapshot: bool,
    ) -> Vec<Self> {
        let ts_us = normalize_ts_us(inc.timestamp).saturating_add(delay_us);
        let is_snapshot = if force_snapshot || inc.is_snapshot {
            1
        } else {
            0
        };
        let mut out = Vec::with_capacity(inc.bids.len() + inc.asks.len());

        for level in inc.bids {
            out.push(Self {
                kind: EventKind::Incremental,
                ts_us,
                side_id: 0,
                is_snapshot,
                price: level.price,
                amount: level.amount,
                origin,
            });
        }

        for level in inc.asks {
            out.push(Self {
                kind: EventKind::Incremental,
                ts_us,
                side_id: 1,
                is_snapshot,
                price: level.price,
                amount: level.amount,
                origin,
            });
        }

        if force_snapshot && !out.is_empty() {
            let last = out.last().unwrap();
            out.push(Self {
                kind: last.kind,
                ts_us: last.ts_us,
                side_id: last.side_id,
                is_snapshot: 0,
                price: last.price,
                amount: last.amount,
                origin: last.origin,
            });
        }

        out
    }

    fn tick(ts_us: i64) -> Self {
        Self {
            kind: EventKind::Tick,
            ts_us,
            side_id: 0,
            is_snapshot: 0,
            price: 0.0,
            amount: 0.0,
            origin: Origin::Open,
        }
    }

    fn as_json_line(&self, symbol: &str) -> String {
        match self.kind {
            EventKind::Tick => {
                format!(
                    "{{\"event\":\"tick\",\"symbol\":\"{}\",\"ts_us\":{},\"origin\":{}}}",
                    symbol, self.ts_us, self.origin as u8
                )
            }
            EventKind::Incremental => format!(
                "{{\"event\":\"inc\",\"symbol\":\"{}\",\"ts_us\":{},\"is_snapshot\":{},\"side_id\":{},\"price\":{},\"amount\":{},\"origin\":{}}}",
                symbol,
                self.ts_us,
                self.is_snapshot,
                self.side_id,
                self.price,
                self.amount,
                self.origin as u8
            ),
            EventKind::Trade => format!(
                "{{\"event\":\"trade\",\"symbol\":\"{}\",\"ts_us\":{},\"side_id\":{},\"price\":{},\"amount\":{},\"origin\":{}}}",
                symbol,
                self.ts_us,
                self.side_id,
                self.price,
                self.amount,
                self.origin as u8
            ),
        }
    }
}

fn build_stream_message(event: &Event) -> ZmqMessage {
    let event_type = match event.kind {
        EventKind::Incremental => EVT_INC,
        EventKind::Trade => EVT_TRADE,
        EventKind::Tick => EVT_TICK,
    };

    let mut msg = ZmqMessage::with_size(28);
    let buf = msg.deref_mut();
    buf[0] = event_type;
    buf[1] = event.side_id;
    buf[2] = event.is_snapshot;
    buf[3] = event.origin as u8;
    buf[4..12].copy_from_slice(&event.ts_us.to_le_bytes());
    buf[12..20].copy_from_slice(&event.price.to_le_bytes());
    buf[20..28].copy_from_slice(&event.amount.to_le_bytes());
    msg
}

fn side_id_from_str(side: &str) -> Option<u8> {
    let side = side.trim().to_ascii_uppercase();
    match side.as_str() {
        "BUY" | "B" => Some(0),
        "SELL" | "S" => Some(1),
        _ => None,
    }
}

fn normalize_ts_us(ts: i64) -> i64 {
    if ts < 10_000_000_000 {
        ts * 1_000_000
    } else if ts < 10_000_000_000_000 {
        ts * 1_000
    } else {
        ts
    }
}

fn log_period_stats(exchange: &str, msg: &PeriodMessage) {
    let period = msg.period;
    let ts = msg.ts;
    let post_ts = msg.post_ts;
    let symbol_count = msg.symbol_infos.len();
    let mut trade_count = 0usize;
    let mut inc_count = 0usize;
    for symbol_info in &msg.symbol_infos {
        trade_count += symbol_info.trades.len();
        inc_count += symbol_info.incs.len();
    }
    info!(
        "pb received exchange={} period={} ts={} post_ts={} symbols={} trades={} incs={}",
        exchange, period, ts, post_ts, symbol_count, trade_count, inc_count
    );
}
