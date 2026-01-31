//! Query history management for the REPL.

use std::fs;
use std::path::PathBuf;

use rustyline::history::DefaultHistory;
use rustyline::{Editor, Helper};

use crate::config::Config;
use crate::error::{CliError, Result};

/// Manages query history persistence.
pub struct HistoryManager {
    history_file: PathBuf,
    #[allow(dead_code)] // Used for configuration reference
    max_size: usize,
}

impl HistoryManager {
    /// Create a new history manager.
    pub fn new(history_file: PathBuf, max_size: usize) -> Self {
        Self {
            history_file,
            max_size,
        }
    }

    /// Load history from file into the editor.
    pub fn load<H: Helper>(&self, rl: &mut Editor<H, DefaultHistory>) -> Result<()> {
        // Ensure the config directory exists
        if let Some(parent) = self.history_file.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent).map_err(|e| {
                    CliError::io_with_source(
                        format!("Failed to create config directory: {}", parent.display()),
                        e,
                    )
                })?;
            }
        }

        // Load history (ignore if file doesn't exist)
        if self.history_file.exists() {
            if let Err(e) = rl.load_history(&self.history_file) {
                // Non-fatal: just log and continue
                eprintln!("Warning: Could not load history: {}", e);
            }
        }

        Ok(())
    }

    /// Save history from the editor to file.
    pub fn save<H: Helper>(&self, rl: &mut Editor<H, DefaultHistory>) -> Result<()> {
        if let Err(e) = rl.save_history(&self.history_file) {
            // Non-fatal: just log
            eprintln!("Warning: Could not save history: {}", e);
        }
        Ok(())
    }

    /// Get the history file path.
    #[allow(dead_code)] // Used for testing
    pub fn history_file(&self) -> &PathBuf {
        &self.history_file
    }

    /// Get the maximum history size.
    #[allow(dead_code)] // Used for testing
    pub fn max_size(&self) -> usize {
        self.max_size
    }
}

impl Default for HistoryManager {
    fn default() -> Self {
        let config = Config::default();
        Self {
            history_file: config.repl.history_file,
            max_size: config.repl.history_size,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_history_manager_creation() {
        let temp_dir = TempDir::new().unwrap();
        let history_file = temp_dir.path().join("history");

        let manager = HistoryManager::new(history_file.clone(), 500);

        assert_eq!(manager.history_file(), &history_file);
        assert_eq!(manager.max_size(), 500);
    }
}
