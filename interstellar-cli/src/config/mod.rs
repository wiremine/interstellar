//! Configuration loading and management.
//!
//! Configuration precedence (highest to lowest):
//! 1. Command-line flags
//! 2. Environment variables
//! 3. Config file (~/.config/interstellar/config.toml or ~/.interstellar.toml)
//! 4. Built-in defaults

pub mod defaults;

use std::fs;
use std::path::PathBuf;

use serde::Deserialize;

use crate::error::{CliError, Result};
use crate::OutputFormat;

/// Query language mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum QueryMode {
    #[default]
    Gql,
    Gremlin,
}

impl std::fmt::Display for QueryMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryMode::Gql => write!(f, "gql"),
            QueryMode::Gremlin => write!(f, "gremlin"),
        }
    }
}

/// REPL-specific configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ReplConfig {
    /// Default query language mode.
    pub default_mode: QueryMode,

    /// Command prefix style ("." or "\\").
    pub command_prefix: String,

    /// History file location.
    pub history_file: PathBuf,

    /// Maximum history entries.
    pub history_size: usize,

    /// Enable syntax highlighting.
    pub highlight: bool,

    /// GQL mode prompt.
    pub prompt_gql: String,

    /// Gremlin mode prompt.
    pub prompt_gremlin: String,

    /// Continuation prompt for multi-line input.
    pub continue_prompt: String,
}

impl Default for ReplConfig {
    fn default() -> Self {
        defaults::repl_config()
    }
}

/// Main configuration structure.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Default output format.
    #[serde(default = "defaults::format")]
    pub format: OutputFormat,

    /// Default result limit.
    #[serde(default = "defaults::limit")]
    pub limit: usize,

    /// Show query execution time.
    #[serde(default = "defaults::timing")]
    pub timing: bool,

    /// REPL settings.
    #[serde(default)]
    #[allow(dead_code)] // Will be used in Phase 2 for REPL
    pub repl: ReplConfig,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            format: defaults::format(),
            limit: defaults::limit(),
            timing: defaults::timing(),
            repl: defaults::repl_config(),
        }
    }
}

impl Config {
    /// Load configuration from file with fallback to defaults.
    ///
    /// Tries to load from:
    /// 1. `~/.config/interstellar/config.toml`
    /// 2. `~/.interstellar.toml`
    /// 3. Falls back to defaults if neither exists
    pub fn load() -> Result<Self> {
        let primary = defaults::config_file();
        let fallback = defaults::fallback_config_file();

        // Try primary config location
        if primary.exists() {
            return Self::load_from_file(&primary);
        }

        // Try fallback location
        if fallback.exists() {
            return Self::load_from_file(&fallback);
        }

        // Use defaults
        Ok(Config::default())
    }

    /// Load configuration from a specific file.
    pub fn load_from_file(path: &PathBuf) -> Result<Self> {
        let content = fs::read_to_string(path).map_err(|e| {
            CliError::io_with_source(format!("Failed to read config file: {}", path.display()), e)
        })?;

        let config: Config = toml::from_str(&content).map_err(|e| {
            CliError::config(format!(
                "Failed to parse config file {}: {}",
                path.display(),
                e
            ))
        })?;

        Ok(config)
    }

    /// Get the config directory path.
    #[allow(dead_code)] // Will be used in Phase 2 for REPL history
    pub fn config_dir() -> PathBuf {
        defaults::config_dir()
    }

    /// Ensure the config directory exists.
    #[allow(dead_code)] // Will be used in Phase 2 for REPL history
    pub fn ensure_config_dir() -> Result<PathBuf> {
        let dir = Self::config_dir();
        if !dir.exists() {
            fs::create_dir_all(&dir).map_err(|e| {
                CliError::io_with_source(
                    format!("Failed to create config directory: {}", dir.display()),
                    e,
                )
            })?;
        }
        Ok(dir)
    }
}

// Custom deserializer for OutputFormat to work with serde
impl<'de> Deserialize<'de> for OutputFormat {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.to_lowercase().as_str() {
            "table" => Ok(OutputFormat::Table),
            "json" => Ok(OutputFormat::Json),
            "csv" => Ok(OutputFormat::Csv),
            _ => Err(serde::de::Error::custom(format!(
                "unknown output format: {}. Expected: table, json, or csv",
                s
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.format, OutputFormat::Table);
        assert_eq!(config.limit, 100);
        assert!(!config.timing);
        assert_eq!(config.repl.default_mode, QueryMode::Gql);
        assert_eq!(config.repl.command_prefix, ".");
    }

    #[test]
    fn test_parse_config() {
        let toml = r#"
            format = "json"
            limit = 50
            timing = true

            [repl]
            default_mode = "gremlin"
            command_prefix = "\\"
            highlight = false
        "#;

        let config: Config = toml::from_str(toml).unwrap();
        assert_eq!(config.format, OutputFormat::Json);
        assert_eq!(config.limit, 50);
        assert!(config.timing);
        assert_eq!(config.repl.default_mode, QueryMode::Gremlin);
        assert_eq!(config.repl.command_prefix, "\\");
        assert!(!config.repl.highlight);
    }
}
