pub mod message_old {
    include!(concat!(env!("OUT_DIR"), "/message_old.rs"));
}

pub use message_old::{IncrementOrderBookInfo, PeriodMessage, SymbolInfo, TradeInfo};
