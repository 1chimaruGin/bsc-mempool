//! bsc-meme-mev runner binary. CLI orchestrates the pipeline:
//! sources → decode → dedupe → fanout → (Day 2+ subscribers).
//!
//! Subcommands:
//!   run         — start the live pipeline
//!   replay      — read a capture file and re-emit records
//!   version     — print build info and exit

use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

mod config;
mod wiring;

#[derive(Parser, Debug)]
#[command(name = "bsc-runner", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Start the live mempool runner.
    Run {
        /// Path to the TOML config file.
        #[arg(short, long, default_value = "config/default.toml")]
        config: PathBuf,
        /// Override log format. "fmt" (default) or "json".
        #[arg(long, default_value = "fmt")]
        log_format: String,
    },
    /// Replay a previously-captured `*.bincode.zst` file.
    Replay {
        /// Path to the capture segment.
        path: PathBuf,
        /// Replay speed multiplier. 1.0 = real time. 0.0 = as fast as possible.
        #[arg(long, default_value_t = 0.0)]
        speed: f64,
    },
    /// Print build version + chain info.
    Version,
}

fn init_logging(format: &str) -> Result<()> {
    use tracing_subscriber::EnvFilter;
    use tracing_subscriber::fmt;

    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,bsc_runner=info,bsc_bus=info,bsc_sources=info"));
    match format {
        "json" => {
            fmt()
                .json()
                .with_env_filter(env_filter)
                .with_target(true)
                .init();
        }
        _ => {
            fmt()
                .with_env_filter(env_filter)
                .with_target(true)
                .init();
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Run { config, log_format } => {
            init_logging(&log_format)?;
            wiring::run(&config).await
        }
        Command::Replay { path, speed } => {
            init_logging("fmt")?;
            wiring::replay(&path, speed).await
        }
        Command::Version => {
            println!("bsc-runner {} (chainId=56, BSC mainnet)", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
    }
}
