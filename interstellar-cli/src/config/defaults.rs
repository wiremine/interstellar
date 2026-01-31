//! Default configuration values.

use std::path::PathBuf;

use crate::config::{QueryMode, ReplConfig};
use crate::OutputFormat;

/// Get the default output format.
pub fn format() -> OutputFormat {
    OutputFormat::Table
}

/// Get the default result limit.
pub fn limit() -> usize {
    100
}

/// Get the default timing setting.
pub fn timing() -> bool {
    false
}

/// Get the default REPL configuration.
pub fn repl_config() -> ReplConfig {
    ReplConfig {
        default_mode: QueryMode::Gql,
        command_prefix: ".".to_string(),
        history_file: history_file(),
        history_size: 1000,
        highlight: true,
        prompt_gql: "gql> ".to_string(),
        prompt_gremlin: "gremlin> ".to_string(),
        continue_prompt: "...> ".to_string(),
    }
}

/// Get the default history file path.
pub fn history_file() -> PathBuf {
    config_dir().join("history")
}

/// Get the configuration directory.
pub fn config_dir() -> PathBuf {
    dirs::config_dir()
        .map(|p| p.join("interstellar"))
        .unwrap_or_else(|| PathBuf::from(".interstellar"))
}

/// Get the primary config file path.
pub fn config_file() -> PathBuf {
    config_dir().join("config.toml")
}

/// Get the fallback config file path.
pub fn fallback_config_file() -> PathBuf {
    dirs::home_dir()
        .map(|p| p.join(".interstellar.toml"))
        .unwrap_or_else(|| PathBuf::from(".interstellar.toml"))
}
