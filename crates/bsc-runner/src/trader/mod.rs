//! BSC paper trader.
//!
//! Subscribes to the bus / kol_watch sink, classifies each KOL hit, and:
//! - On BUY: opens TWO paper positions (FastTip + NormalTip) against the
//!   token, sized as `size_fraction × kol_bnb_input`. Entry price is
//!   simulated against PancakeSwap V2 (preferred) / V3 (fallback).
//! - On SELL (or 24h timeout): closes the corresponding open positions.
//!
//! Day-3 scope (this commit):
//!   - [x] types — Side, PortfolioMode, Decision, OpenPosition, ClosedTrade
//!   - [x] position — PositionBook with average-in / FIFO close
//!   - [x] ledger  — closed_trades.csv + open_positions.json (atomic)
//!   - [x] sim     — V2-first / V3-fallback via bsc-dex
//!   - [ ] strategy — entry/exit decision logic (next commit)
//!   - [ ] paper    — executor (next commit)
//!   - [ ] wiring  — TraderConfig, run_consumer, sweeper, daily summary
//!                   (next commit)

pub mod ledger;
pub mod position;
pub mod sim;
pub mod types;

pub use ledger::Ledger;
pub use position::PositionBook;
pub use sim::{QuoteResult, QuoteVenue, Simulator};
pub use types::{
    CloseReason, ClosedTrade, Decision, OpenPosition, PortfolioMode, PositionKey, Side,
    SkipReason, Token, DEFAULT_HOLD_TIMEOUT,
};
