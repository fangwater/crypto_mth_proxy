use crate::proto::PeriodMessage;
use log::{debug, warn};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

pub struct PeriodAligner {
    open: BTreeMap<i64, PeriodMessage>,
    hedge: BTreeMap<i64, PeriodMessage>,
    warn_interval: Duration,
    last_warn: Option<Instant>,
    last_status: Option<Instant>,
}

impl PeriodAligner {
    pub fn new(warn_interval: Duration) -> Self {
        Self {
            open: BTreeMap::new(),
            hedge: BTreeMap::new(),
            warn_interval,
            last_warn: None,
            last_status: None,
        }
    }

    pub fn insert_open(&mut self, msg: PeriodMessage) -> Option<(PeriodMessage, PeriodMessage)> {
        let period = msg.period;
        self.open.insert(period, msg);
        self.try_take_min_pair()
    }

    pub fn insert_hedge(&mut self, msg: PeriodMessage) -> Option<(PeriodMessage, PeriodMessage)> {
        let period = msg.period;
        self.hedge.insert(period, msg);
        self.try_take_min_pair()
    }

    pub fn warn_if_gap(&mut self, open_venue: &str, hedge_venue: &str) {
        let Some(open_period) = self.open.keys().next().copied() else {
            return;
        };
        let Some(hedge_period) = self.hedge.keys().next().copied() else {
            return;
        };

        if open_period > hedge_period + 1 || hedge_period > open_period + 1 {
            let now = Instant::now();
            let should_warn = self
                .last_warn
                .map(|last| now.duration_since(last) >= self.warn_interval)
                .unwrap_or(true);
            if should_warn {
                warn!(
                    "period gap open_venue={} hedge_venue={} open_period={} hedge_period={}",
                    open_venue, hedge_venue, open_period, hedge_period
                );
                self.last_warn = Some(now);
            }
        }
    }

    pub fn log_status(&mut self, open_venue: &str, hedge_venue: &str) {
        let now = Instant::now();
        let should_log = self
            .last_status
            .map(|last| now.duration_since(last) >= self.warn_interval)
            .unwrap_or(true);
        if !should_log {
            return;
        }

        let open_period = self.open.keys().next().copied();
        let hedge_period = self.hedge.keys().next().copied();
        debug!(
            "period status open_venue={} hedge_venue={} open_period={:?} hedge_period={:?} open_buf={} hedge_buf={}",
            open_venue,
            hedge_venue,
            open_period,
            hedge_period,
            self.open.len(),
            self.hedge.len()
        );
        self.last_status = Some(now);
    }

    fn try_take_min_pair(&mut self) -> Option<(PeriodMessage, PeriodMessage)> {
        let open_period = *self.open.keys().next()?;
        let hedge_period = *self.hedge.keys().next()?;
        if open_period != hedge_period {
            return None;
        }
        let open_msg = self.open.remove(&open_period)?;
        let hedge_msg = self.hedge.remove(&hedge_period)?;
        Some((open_msg, hedge_msg))
    }
}
