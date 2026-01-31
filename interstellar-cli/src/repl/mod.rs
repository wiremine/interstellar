//! Interactive REPL (Read-Eval-Print Loop) for Interstellar CLI.
//!
//! Provides an interactive query session with history, tab completion,
//! and syntax highlighting.

mod completer;
mod highlighter;
mod history;

use std::borrow::Cow;
use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use rustyline::error::ReadlineError;
use rustyline::highlight::Highlighter;
use rustyline::hint::HistoryHinter;
use rustyline::history::History;
use rustyline::validate::MatchingBracketValidator;
use rustyline::{Completer, Config, Editor, Helper, Hinter, Validator};

use crate::config::{QueryMode, ReplConfig};
use crate::error::{CliError, Result};
use crate::format::{format_results, OutputFormat, QueryResult};
use crate::gremlin::{
    format_result, GremlinEngine, GremlinVariableContext, PersistentGremlinEngine,
};
use crate::OutputFormat as CliOutputFormat;
use interstellar::gremlin::ExecutionResult;
use interstellar::storage::GraphStorage;

pub use completer::ReplCompleter;
pub use highlighter::QueryHighlighter;
pub use history::HistoryManager;

/// REPL helper combining completion, hints, highlighting, and validation.
#[derive(Helper, Completer, Hinter, Validator)]
pub struct ReplHelper {
    #[rustyline(Completer)]
    pub completer: ReplCompleter,
    #[rustyline(Hinter)]
    pub hinter: HistoryHinter,
    #[rustyline(Validator)]
    pub validator: MatchingBracketValidator,
    pub highlighter: QueryHighlighter,
}

impl Highlighter for ReplHelper {
    fn highlight<'l>(&self, line: &'l str, pos: usize) -> Cow<'l, str> {
        if self.highlighter.is_enabled() {
            self.highlighter.highlight(line, pos)
        } else {
            Cow::Borrowed(line)
        }
    }

    fn highlight_prompt<'b, 's: 'b, 'p: 'b>(
        &'s self,
        prompt: &'p str,
        _default: bool,
    ) -> Cow<'b, str> {
        use colored::Colorize;
        Cow::Owned(prompt.bold().to_string())
    }

    fn highlight_hint<'h>(&self, hint: &'h str) -> Cow<'h, str> {
        use colored::Colorize;
        Cow::Owned(hint.dimmed().to_string())
    }

    fn highlight_char(&self, _line: &str, _pos: usize, _forced: bool) -> bool {
        true
    }
}

/// Runtime settings that can be changed during the REPL session.
#[derive(Debug, Clone)]
pub struct RuntimeSettings {
    pub format: OutputFormat,
    pub limit: usize,
    pub timing: bool,
    pub output_file: Option<PathBuf>,
}

impl RuntimeSettings {
    pub fn new(format: CliOutputFormat, limit: usize, timing: bool) -> Self {
        Self {
            format: format.into(),
            limit,
            timing,
            output_file: None,
        }
    }
}

/// Labels available in the graph.
#[derive(Debug, Clone, Default)]
pub struct GraphLabels {
    pub vertex_labels: Vec<String>,
    pub edge_labels: Vec<String>,
}

/// Basic graph statistics.
#[derive(Debug, Clone, Default)]
pub struct GraphStats {
    pub vertex_count: u64,
    pub edge_count: u64,
}

/// The main REPL structure.
pub struct Repl {
    gremlin_engine: GremlinEngine,
    config: ReplConfig,
    settings: RuntimeSettings,
    mode: QueryMode,
    history_manager: HistoryManager,
    /// Shared flag to update highlighter mode
    highlighter_mode: Option<Arc<AtomicBool>>,
    /// Variable context for Gremlin scripts (persists across commands)
    gremlin_context: GremlinVariableContext,
}

impl Repl {
    /// Create a new REPL instance.
    pub fn new(graph: interstellar::Graph, config: ReplConfig, settings: RuntimeSettings) -> Self {
        let mode = config.default_mode;
        let history_manager = HistoryManager::new(config.history_file.clone(), config.history_size);
        let gremlin_engine = GremlinEngine::new(graph);

        Self {
            gremlin_engine,
            config,
            settings,
            mode,
            history_manager,
            highlighter_mode: None,
            gremlin_context: GremlinVariableContext::new(),
        }
    }

    /// Get a reference to the underlying graph.
    fn graph(&self) -> &interstellar::Graph {
        self.gremlin_engine.graph()
    }

    /// Get labels from the graph.
    fn get_labels(&self) -> GraphLabels {
        use std::collections::HashSet;

        let mut vertex_labels: HashSet<String> = HashSet::new();
        let mut edge_labels: HashSet<String> = HashSet::new();

        let snapshot = self.graph().snapshot();
        for vertex in snapshot.all_vertices() {
            vertex_labels.insert(vertex.label.clone());
        }

        for edge in snapshot.all_edges() {
            edge_labels.insert(edge.label.clone());
        }

        GraphLabels {
            vertex_labels: vertex_labels.into_iter().collect(),
            edge_labels: edge_labels.into_iter().collect(),
        }
    }

    /// Get basic stats from the graph.
    fn get_stats(&self) -> GraphStats {
        GraphStats {
            vertex_count: self.graph().vertex_count(),
            edge_count: self.graph().edge_count(),
        }
    }

    /// Execute a GQL query.
    fn execute_gql(&self, query: &str) -> Result<Vec<interstellar::Value>> {
        self.graph().gql(query).map_err(|e| {
            let msg = e.to_string();
            if msg.contains("parse") || msg.contains("Parse") || msg.contains("syntax") {
                CliError::query_syntax(msg)
            } else {
                CliError::query_execution(msg)
            }
        })
    }

    /// Run the REPL loop.
    pub fn run(&mut self) -> Result<()> {
        // Configure rustyline
        let rl_config = Config::builder()
            .history_ignore_dups(true)
            .map_err(|e| CliError::general(format!("Failed to configure readline: {}", e)))?
            .history_ignore_space(true)
            .max_history_size(self.config.history_size)
            .map_err(|e| CliError::general(format!("Failed to configure history: {}", e)))?
            .auto_add_history(true)
            .build();

        // Create helper with completion and highlighting
        let highlighter = QueryHighlighter::new(self.config.highlight, self.mode);
        self.highlighter_mode = Some(highlighter.mode_flag());

        let helper = ReplHelper {
            completer: ReplCompleter::new(self.get_labels()),
            hinter: HistoryHinter::new(),
            validator: MatchingBracketValidator::new(),
            highlighter,
        };

        let mut rl: Editor<ReplHelper, _> = Editor::with_config(rl_config)
            .map_err(|e| CliError::general(format!("Failed to create editor: {}", e)))?;
        rl.set_helper(Some(helper));

        // Load history
        self.history_manager.load(&mut rl)?;

        // Print welcome message
        self.print_welcome();

        // Multi-line input buffer
        let mut input_buffer = String::new();

        loop {
            let prompt = if input_buffer.is_empty() {
                self.get_prompt()
            } else {
                self.config.continue_prompt.clone()
            };

            match rl.readline(&prompt) {
                Ok(line) => {
                    let line = line.trim();

                    // Handle empty input
                    if line.is_empty() && input_buffer.is_empty() {
                        continue;
                    }

                    // Check for dot-commands (only when not in multi-line mode)
                    if input_buffer.is_empty() && self.is_dot_command(line) {
                        if let Err(e) = self.handle_dot_command(line, &mut rl) {
                            eprintln!("Error: {}", e);
                        }
                        continue;
                    }

                    // Append to input buffer
                    if !input_buffer.is_empty() {
                        input_buffer.push('\n');
                    }
                    input_buffer.push_str(line);

                    // Check if query is complete (ends with semicolon for GQL)
                    if self.is_query_complete(&input_buffer) {
                        // Execute the query
                        if let Err(e) = self.execute_query(&input_buffer) {
                            eprintln!("Error: {}", e);
                        }
                        input_buffer.clear();
                    }
                }
                Err(ReadlineError::Interrupted) => {
                    // Ctrl+C: cancel current input
                    if !input_buffer.is_empty() {
                        input_buffer.clear();
                        println!("^C");
                    } else {
                        println!("Use .quit or .exit to exit");
                    }
                }
                Err(ReadlineError::Eof) => {
                    // Ctrl+D: exit
                    println!("Goodbye!");
                    break;
                }
                Err(err) => {
                    eprintln!("Error: {}", err);
                    break;
                }
            }
        }

        // Save history
        self.history_manager.save(&mut rl)?;

        Ok(())
    }

    /// Check if the line is a dot-command.
    fn is_dot_command(&self, line: &str) -> bool {
        let prefix = &self.config.command_prefix;
        line.starts_with(prefix)
    }

    /// Check if a query is complete (ready to execute).
    fn is_query_complete(&self, query: &str) -> bool {
        let trimmed = query.trim();

        match self.mode {
            QueryMode::Gql => {
                // GQL queries should end with semicolon
                // Also handle single-line queries without semicolon
                trimmed.ends_with(';') || self.is_simple_query(trimmed)
            }
            QueryMode::Gremlin => {
                // Gremlin statements:
                // - Variable assignment: "alice = g.addV(...).next()" ends with )
                // - Traversal: "g.V().toList()" ends with )
                // Multi-line is supported - each line can be a statement
                trimmed.ends_with(')')
            }
        }
    }

    /// Check if this is a simple single-line query.
    fn is_simple_query(&self, query: &str) -> bool {
        // Simple queries: single line without common multi-line keywords at start
        let upper = query.to_uppercase();
        !query.contains('\n') && !upper.ends_with("WHERE") && !upper.ends_with("RETURN")
    }

    /// Get the current prompt based on mode.
    fn get_prompt(&self) -> String {
        match self.mode {
            QueryMode::Gql => self.config.prompt_gql.clone(),
            QueryMode::Gremlin => self.config.prompt_gremlin.clone(),
        }
    }

    /// Print welcome message.
    fn print_welcome(&self) {
        println!("Interstellar CLI v{}", env!("CARGO_PKG_VERSION"));
        println!("Type .help for commands, .quit to exit");
        if self.mode == QueryMode::Gremlin {
            println!(
                "Gremlin variables persist across commands (e.g., alice = g.addV(...).next())"
            );
        }
        println!();
    }

    /// Handle a dot-command.
    fn handle_dot_command<H: Helper>(
        &mut self,
        line: &str,
        rl: &mut Editor<H, rustyline::history::DefaultHistory>,
    ) -> Result<()> {
        let prefix = &self.config.command_prefix;
        let without_prefix = line.strip_prefix(prefix).unwrap_or(line);
        let parts: Vec<&str> = without_prefix.split_whitespace().collect();
        let command = parts.first().map(|s| s.to_lowercase()).unwrap_or_default();

        match command.as_str() {
            "help" | "h" | "?" => self.cmd_help(),
            "schema" => self.cmd_schema(),
            "stats" => self.cmd_stats(),
            "history" => self.cmd_history(rl),
            "vars" | "variables" => self.cmd_vars(),
            "mode" => {
                let arg = parts.get(1).copied();
                self.cmd_mode(arg)
            }
            "clear" | "cls" => self.cmd_clear(),
            "quit" | "exit" | "q" => self.cmd_quit(),
            "set" => {
                let key = parts.get(1).copied();
                let value = parts.get(2).copied();
                self.cmd_set(key, value)
            }
            "read" => {
                let file = parts.get(1).copied();
                self.cmd_read(file)
            }
            "output" => {
                let file = parts.get(1).copied();
                self.cmd_output(file)
            }
            _ => {
                eprintln!(
                    "Unknown command: {}. Type .help for available commands.",
                    command
                );
                Ok(())
            }
        }
    }

    // === Dot-command implementations ===

    fn cmd_help(&self) -> Result<()> {
        let prefix = &self.config.command_prefix;
        println!("Commands:");
        println!("  {}help              Show this help", prefix);
        println!("  {}schema            Display database schema", prefix);
        println!("  {}stats             Show database statistics", prefix);
        println!("  {}history           Show query history", prefix);
        println!(
            "  {}vars              Show Gremlin session variables",
            prefix
        );
        println!(
            "  {}mode <lang>       Switch language mode (gql | gremlin)",
            prefix
        );
        println!("  {}clear             Clear screen", prefix);
        println!("  {}quit / {}exit      Exit REPL", prefix, prefix);
        println!(
            "  {}set <key> <val>   Set option (format, limit, timing)",
            prefix
        );
        println!("  {}read <file>       Execute commands from file", prefix);
        println!(
            "  {}output <file>     Redirect output to file ({}output stdout to reset)",
            prefix, prefix
        );
        println!();
        println!("Current mode: {:?}", self.mode);
        Ok(())
    }

    fn cmd_vars(&self) -> Result<()> {
        let vars: Vec<_> = self.gremlin_context.variables().collect();
        if vars.is_empty() {
            println!("No Gremlin variables defined.");
            println!(
                "Use assignment syntax: alice = g.addV('person').property('name', 'Alice').next()"
            );
        } else {
            println!("Gremlin session variables:");
            for name in vars {
                if let Some(value) = self.gremlin_context.get(name) {
                    println!("  {} = {:?}", name, value);
                }
            }
        }
        Ok(())
    }

    fn cmd_schema(&self) -> Result<()> {
        // Schema inference is Phase 5, show basic info for now
        let labels = self.get_labels();
        println!("Vertex Labels:");
        for label in &labels.vertex_labels {
            println!("  {}", label);
        }
        println!();
        println!("Edge Labels:");
        for label in &labels.edge_labels {
            println!("  {}", label);
        }
        Ok(())
    }

    fn cmd_stats(&self) -> Result<()> {
        let stats = self.get_stats();
        println!("Vertices: {}", stats.vertex_count);
        println!("Edges: {}", stats.edge_count);
        Ok(())
    }

    fn cmd_history<H: Helper>(
        &self,
        rl: &Editor<H, rustyline::history::DefaultHistory>,
    ) -> Result<()> {
        let history = rl.history();
        if history.is_empty() {
            println!("(no history)");
            return Ok(());
        }

        for (i, entry) in history.iter().enumerate() {
            println!("{:4}  {}", i + 1, entry);
        }
        Ok(())
    }

    fn cmd_mode(&mut self, arg: Option<&str>) -> Result<()> {
        match arg {
            Some("gql") => {
                self.mode = QueryMode::Gql;
                if let Some(ref mode_flag) = self.highlighter_mode {
                    mode_flag.store(false, Ordering::SeqCst);
                }
                println!("Switched to GQL mode");
            }
            Some("gremlin") => {
                self.mode = QueryMode::Gremlin;
                if let Some(ref mode_flag) = self.highlighter_mode {
                    mode_flag.store(true, Ordering::SeqCst);
                }
                println!("Switched to Gremlin mode");
            }
            Some(other) => {
                eprintln!("Unknown mode: {}. Use 'gql' or 'gremlin'", other);
            }
            None => {
                println!("Current mode: {}", self.mode);
                println!("Usage: .mode <gql|gremlin>");
            }
        }
        Ok(())
    }

    fn cmd_clear(&self) -> Result<()> {
        // ANSI escape code to clear screen and move cursor to top
        print!("\x1B[2J\x1B[1;1H");
        std::io::stdout().flush().ok();
        Ok(())
    }

    fn cmd_quit(&self) -> Result<()> {
        println!("Goodbye!");
        std::process::exit(0);
    }

    fn cmd_set(&mut self, key: Option<&str>, value: Option<&str>) -> Result<()> {
        match (key, value) {
            (Some("format"), Some(v)) => {
                match v.to_lowercase().as_str() {
                    "table" => self.settings.format = OutputFormat::Table,
                    "json" => self.settings.format = OutputFormat::Json,
                    "csv" => self.settings.format = OutputFormat::Csv,
                    _ => eprintln!("Unknown format: {}. Use table, json, or csv", v),
                }
                println!("Format set to {:?}", self.settings.format);
            }
            (Some("limit"), Some(v)) => match v.parse::<usize>() {
                Ok(n) => {
                    self.settings.limit = n;
                    println!("Limit set to {}", n);
                }
                Err(_) => eprintln!("Invalid limit value: {}", v),
            },
            (Some("timing"), Some(v)) => match v.to_lowercase().as_str() {
                "true" | "on" | "1" | "yes" => {
                    self.settings.timing = true;
                    println!("Timing enabled");
                }
                "false" | "off" | "0" | "no" => {
                    self.settings.timing = false;
                    println!("Timing disabled");
                }
                _ => eprintln!("Invalid value: {}. Use true/false or on/off", v),
            },
            (Some(k), None) => {
                eprintln!("Missing value for setting '{}'", k);
            }
            (None, _) => {
                println!("Current settings:");
                println!("  format = {:?}", self.settings.format);
                println!("  limit = {}", self.settings.limit);
                println!("  timing = {}", self.settings.timing);
                println!();
                println!("Usage: .set <key> <value>");
            }
            _ => {}
        }
        Ok(())
    }

    fn cmd_read(&mut self, file: Option<&str>) -> Result<()> {
        let path = match file {
            Some(f) => PathBuf::from(f),
            None => {
                eprintln!("Usage: .read <file>");
                return Ok(());
            }
        };

        let content = fs::read_to_string(&path).map_err(|e| {
            CliError::io_with_source(format!("Failed to read file: {}", path.display()), e)
        })?;

        // Execute each line
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with("--") || line.starts_with("//") {
                continue;
            }

            println!("> {}", line);
            if let Err(e) = self.execute_query(line) {
                eprintln!("Error: {}", e);
            }
        }

        Ok(())
    }

    fn cmd_output(&mut self, file: Option<&str>) -> Result<()> {
        match file {
            Some("stdout") | None if self.settings.output_file.is_some() => {
                self.settings.output_file = None;
                println!("Output reset to stdout");
            }
            Some(f) if f != "stdout" => {
                self.settings.output_file = Some(PathBuf::from(f));
                println!("Output redirected to: {}", f);
            }
            None => {
                match &self.settings.output_file {
                    Some(f) => println!("Output: {}", f.display()),
                    None => println!("Output: stdout"),
                }
                println!("Usage: .output <file> or .output stdout");
            }
            _ => {}
        }
        Ok(())
    }

    /// Execute a query and display results.
    fn execute_query(&mut self, query: &str) -> Result<()> {
        let query = query.trim().trim_end_matches(';');
        if query.is_empty() {
            return Ok(());
        }

        let start = Instant::now();

        match self.mode {
            QueryMode::Gql => {
                let results = self.execute_gql(query)?;
                let elapsed = start.elapsed();

                let query_result = QueryResult::new(
                    results,
                    if self.settings.timing {
                        Some(elapsed)
                    } else {
                        None
                    },
                );

                self.output_results(&query_result)?;
            }
            QueryMode::Gremlin => {
                // Use execute_with_context to maintain variables across commands
                let context = std::mem::take(&mut self.gremlin_context);
                let script_result = self.gremlin_engine.execute_with_context(query, context)?;
                let elapsed = start.elapsed();

                // Update the context with any new variables
                self.gremlin_context = script_result.variables;

                // Format the Gremlin result (skip Unit results from assignments)
                if !matches!(script_result.result, ExecutionResult::Unit) {
                    let output = format_result(&script_result.result);

                    match &self.settings.output_file {
                        Some(path) => {
                            let mut file = File::create(path).map_err(|e| {
                                CliError::io_with_source(
                                    format!("Failed to open output file: {}", path.display()),
                                    e,
                                )
                            })?;
                            writeln!(file, "{}", output).map_err(|e| {
                                CliError::io_with_source("Failed to write to output file", e)
                            })?;
                        }
                        None => {
                            println!("{}", output);
                        }
                    }
                }

                if self.settings.timing {
                    println!("Time: {:.3}ms", elapsed.as_secs_f64() * 1000.0);
                }
            }
        }

        Ok(())
    }

    /// Output results to the configured destination.
    fn output_results(&self, result: &QueryResult) -> Result<()> {
        let output = format_results(result, self.settings.format, self.settings.limit);

        match &self.settings.output_file {
            Some(path) => {
                let mut file = File::create(path).map_err(|e| {
                    CliError::io_with_source(
                        format!("Failed to open output file: {}", path.display()),
                        e,
                    )
                })?;
                writeln!(file, "{}", output)
                    .map_err(|e| CliError::io_with_source("Failed to write to output file", e))?;
            }
            None => {
                println!("{}", output);
            }
        }

        Ok(())
    }
}

/// Start the REPL with the given graph and settings.
pub fn start(
    graph: interstellar::Graph,
    config: ReplConfig,
    settings: RuntimeSettings,
) -> Result<()> {
    let mut repl = Repl::new(graph, config, settings);
    repl.run()
}

/// Start the REPL with a persistent graph (GQL only, no Gremlin scripting).
pub fn start_persistent(
    graph: interstellar::storage::PersistentGraph,
    config: ReplConfig,
    settings: RuntimeSettings,
) -> Result<()> {
    let mut repl = PersistentRepl::new(graph, config, settings);
    repl.run()
}

// ============================================================================
// PersistentRepl - REPL for PersistentGraph (GQL and Gremlin)
// ============================================================================

/// The REPL structure for persistent graphs.
///
/// Supports both GQL queries and Gremlin scripting with variable context.
pub struct PersistentRepl {
    gremlin_engine: PersistentGremlinEngine,
    gremlin_context: GremlinVariableContext,
    config: ReplConfig,
    settings: RuntimeSettings,
    mode: QueryMode,
    history_manager: HistoryManager,
    /// Shared flag to update highlighter mode
    highlighter_mode: Option<Arc<AtomicBool>>,
}

impl PersistentRepl {
    /// Create a new PersistentRepl instance.
    pub fn new(
        graph: interstellar::storage::PersistentGraph,
        config: ReplConfig,
        settings: RuntimeSettings,
    ) -> Self {
        let mode = config.default_mode;
        let history_manager = HistoryManager::new(config.history_file.clone(), config.history_size);
        let gremlin_engine = PersistentGremlinEngine::new(graph);

        Self {
            gremlin_engine,
            gremlin_context: GremlinVariableContext::new(),
            config,
            settings,
            mode,
            history_manager,
            highlighter_mode: None,
        }
    }

    /// Get a reference to the underlying graph.
    fn graph(&self) -> &interstellar::storage::PersistentGraph {
        self.gremlin_engine.graph()
    }

    /// Get labels from the graph.
    fn get_labels(&self) -> GraphLabels {
        use std::collections::HashSet;

        let mut vertex_labels: HashSet<String> = HashSet::new();
        let mut edge_labels: HashSet<String> = HashSet::new();

        let snapshot = self.graph().snapshot();
        for vertex in snapshot.all_vertices() {
            vertex_labels.insert(vertex.label.clone());
        }

        for edge in snapshot.all_edges() {
            edge_labels.insert(edge.label.clone());
        }

        GraphLabels {
            vertex_labels: vertex_labels.into_iter().collect(),
            edge_labels: edge_labels.into_iter().collect(),
        }
    }

    /// Get basic stats from the graph.
    fn get_stats(&self) -> GraphStats {
        GraphStats {
            vertex_count: self.graph().vertex_count(),
            edge_count: self.graph().edge_count(),
        }
    }

    /// Execute a GQL query.
    fn execute_gql(&self, query: &str) -> Result<Vec<interstellar::Value>> {
        self.graph().gql(query).map_err(|e| {
            let msg = e.to_string();
            if msg.contains("parse") || msg.contains("Parse") || msg.contains("syntax") {
                CliError::query_syntax(msg)
            } else {
                CliError::query_execution(msg)
            }
        })
    }

    /// Get the current prompt based on mode.
    fn get_prompt(&self) -> String {
        match self.mode {
            QueryMode::Gql => self.config.prompt_gql.clone(),
            QueryMode::Gremlin => self.config.prompt_gremlin.clone(),
        }
    }

    /// Run the REPL loop.
    pub fn run(&mut self) -> Result<()> {
        // Configure rustyline
        let rl_config = Config::builder()
            .history_ignore_dups(true)
            .map_err(|e| CliError::general(format!("Failed to configure readline: {}", e)))?
            .history_ignore_space(true)
            .max_history_size(self.config.history_size)
            .map_err(|e| CliError::general(format!("Failed to configure history: {}", e)))?
            .auto_add_history(true)
            .build();

        // Create helper with completion and highlighting
        let highlighter = QueryHighlighter::new(self.config.highlight, self.mode);
        self.highlighter_mode = Some(highlighter.mode_flag());

        let helper = ReplHelper {
            completer: ReplCompleter::new(self.get_labels()),
            hinter: HistoryHinter::new(),
            validator: MatchingBracketValidator::new(),
            highlighter,
        };

        let mut rl: Editor<ReplHelper, _> = Editor::with_config(rl_config)
            .map_err(|e| CliError::general(format!("Failed to create editor: {}", e)))?;
        rl.set_helper(Some(helper));

        // Load history
        self.history_manager.load(&mut rl)?;

        // Print welcome message
        self.print_welcome();

        // Multi-line input buffer
        let mut input_buffer = String::new();

        loop {
            let prompt = if input_buffer.is_empty() {
                self.get_prompt()
            } else {
                self.config.continue_prompt.clone()
            };

            match rl.readline(&prompt) {
                Ok(line) => {
                    let line = line.trim();

                    // Handle empty input
                    if line.is_empty() && input_buffer.is_empty() {
                        continue;
                    }

                    // Check for dot-commands (only when not in multi-line mode)
                    if input_buffer.is_empty() && self.is_dot_command(line) {
                        if let Err(e) = self.handle_dot_command(line, &mut rl) {
                            eprintln!("Error: {}", e);
                        }
                        continue;
                    }

                    // Append to input buffer
                    if !input_buffer.is_empty() {
                        input_buffer.push('\n');
                    }
                    input_buffer.push_str(line);

                    // Check if query is complete (ends with semicolon for GQL)
                    if self.is_query_complete(&input_buffer) {
                        // Execute the query
                        if let Err(e) = self.execute_query(&input_buffer) {
                            eprintln!("Error: {}", e);
                        }
                        input_buffer.clear();
                    }
                }
                Err(ReadlineError::Interrupted) => {
                    // Ctrl+C: cancel current input
                    if !input_buffer.is_empty() {
                        input_buffer.clear();
                        println!("^C");
                    } else {
                        println!("Use .quit or .exit to exit");
                    }
                }
                Err(ReadlineError::Eof) => {
                    // Ctrl+D: exit
                    println!("Goodbye!");
                    break;
                }
                Err(err) => {
                    eprintln!("Error: {}", err);
                    break;
                }
            }
        }

        // Save history
        self.history_manager.save(&mut rl)?;

        Ok(())
    }

    /// Check if the line is a dot-command.
    fn is_dot_command(&self, line: &str) -> bool {
        let prefix = &self.config.command_prefix;
        line.starts_with(prefix)
    }

    /// Check if a query is complete (ready to execute).
    fn is_query_complete(&self, query: &str) -> bool {
        let trimmed = query.trim();

        match self.mode {
            QueryMode::Gql => {
                // GQL queries should end with semicolon
                // Also handle single-line queries without semicolon
                trimmed.ends_with(';') || self.is_simple_query(trimmed)
            }
            QueryMode::Gremlin => {
                // Standard Gremlin: must end with ) for complete traversal
                // Terminal steps like toList(), next(), iterate() end with )
                // Non-terminal queries also end with ) and will default to toList() behavior
                trimmed.ends_with(')')
            }
        }
    }

    /// Check if this is a simple single-line query.
    fn is_simple_query(&self, query: &str) -> bool {
        let upper = query.to_uppercase();
        !query.contains('\n') && !upper.ends_with("WHERE") && !upper.ends_with("RETURN")
    }

    /// Print welcome message.
    fn print_welcome(&self) {
        println!(
            "Interstellar CLI v{} (Persistent Mode)",
            env!("CARGO_PKG_VERSION")
        );
        println!("Type .help for commands, .quit to exit");
        println!();
    }

    /// Handle a dot-command.
    fn handle_dot_command<H: Helper>(
        &mut self,
        line: &str,
        rl: &mut Editor<H, rustyline::history::DefaultHistory>,
    ) -> Result<()> {
        let prefix = &self.config.command_prefix;
        let without_prefix = line.strip_prefix(prefix).unwrap_or(line);
        let parts: Vec<&str> = without_prefix.split_whitespace().collect();
        let command = parts.first().map(|s| s.to_lowercase()).unwrap_or_default();

        match command.as_str() {
            "help" | "h" | "?" => self.cmd_help(),
            "schema" => self.cmd_schema(),
            "stats" => self.cmd_stats(),
            "history" => self.cmd_history(rl),
            "vars" | "variables" => self.cmd_vars(),
            "mode" => {
                let arg = parts.get(1).copied();
                self.cmd_mode(arg)
            }
            "clear" | "cls" => self.cmd_clear(),
            "quit" | "exit" | "q" => self.cmd_quit(),
            "set" => {
                let key = parts.get(1).copied();
                let value = parts.get(2).copied();
                self.cmd_set(key, value)
            }
            "read" => {
                let file = parts.get(1).copied();
                self.cmd_read(file)
            }
            "output" => {
                let file = parts.get(1).copied();
                self.cmd_output(file)
            }
            "queries" => self.cmd_queries(),
            "query" => {
                let name = parts.get(1).copied();
                self.cmd_query(name)
            }
            "save" => {
                // .save <name> <query>
                // The query may contain spaces, so join all parts after the name
                let name = parts.get(1).copied();
                let query_text = if parts.len() > 2 {
                    Some(parts[2..].join(" "))
                } else {
                    None
                };
                self.cmd_save(name, query_text.as_deref())
            }
            "delete" => {
                let name = parts.get(1).copied();
                self.cmd_delete_query(name)
            }
            "run" => {
                let name = parts.get(1).copied();
                self.cmd_run_query(name)
            }
            _ => {
                eprintln!(
                    "Unknown command: {}. Type .help for available commands.",
                    command
                );
                Ok(())
            }
        }
    }

    // === Dot-command implementations ===

    fn cmd_help(&self) -> Result<()> {
        let prefix = &self.config.command_prefix;
        println!("Commands:");
        println!("  {}help              Show this help", prefix);
        println!("  {}schema            Display database schema", prefix);
        println!("  {}stats             Show database statistics", prefix);
        println!("  {}history           Show query history", prefix);
        println!(
            "  {}vars              Show Gremlin session variables",
            prefix
        );
        println!(
            "  {}mode <lang>       Switch language mode (gql | gremlin)",
            prefix
        );
        println!("  {}clear             Clear screen", prefix);
        println!("  {}quit / {}exit      Exit REPL", prefix, prefix);
        println!(
            "  {}set <key> <val>   Set option (format, limit, timing)",
            prefix
        );
        println!("  {}read <file>       Execute commands from file", prefix);
        println!(
            "  {}output <file>     Redirect output to file ({}output stdout to reset)",
            prefix, prefix
        );
        println!();
        println!("Saved Query Commands:");
        println!("  {}queries           List all saved queries", prefix);
        println!("  {}query <name>      Show a saved query", prefix);
        println!(
            "  {}save <name> <q>   Save current mode query (use quotes for spaces)",
            prefix
        );
        println!("  {}delete <name>     Delete a saved query", prefix);
        println!("  {}run <name>        Execute a saved query", prefix);
        println!();
        println!("Current mode: {}", self.mode);
        println!(
            "Format: {:?}, Limit: {}, Timing: {}",
            self.settings.format, self.settings.limit, self.settings.timing
        );
        Ok(())
    }

    fn cmd_vars(&self) -> Result<()> {
        let vars: Vec<_> = self.gremlin_context.variables().collect();
        if vars.is_empty() {
            println!("No Gremlin variables defined.");
            println!(
                "Use assignment syntax: alice = g.addV('person').property('name', 'Alice').next()"
            );
        } else {
            println!("Gremlin session variables:");
            for name in vars {
                if let Some(value) = self.gremlin_context.get(name) {
                    println!("  {} = {:?}", name, value);
                }
            }
        }
        Ok(())
    }

    fn cmd_mode(&mut self, arg: Option<&str>) -> Result<()> {
        match arg {
            Some("gql") => {
                self.mode = QueryMode::Gql;
                if let Some(ref mode_flag) = self.highlighter_mode {
                    mode_flag.store(false, Ordering::SeqCst);
                }
                println!("Switched to GQL mode");
            }
            Some("gremlin") => {
                self.mode = QueryMode::Gremlin;
                if let Some(ref mode_flag) = self.highlighter_mode {
                    mode_flag.store(true, Ordering::SeqCst);
                }
                println!("Switched to Gremlin mode");
            }
            Some(other) => {
                eprintln!("Unknown mode: {}. Use 'gql' or 'gremlin'", other);
            }
            None => {
                println!("Current mode: {}", self.mode);
                println!("Usage: .mode <gql|gremlin>");
            }
        }
        Ok(())
    }

    fn cmd_schema(&self) -> Result<()> {
        let labels = self.get_labels();
        println!("Vertex Labels:");
        for label in &labels.vertex_labels {
            println!("  {}", label);
        }
        println!();
        println!("Edge Labels:");
        for label in &labels.edge_labels {
            println!("  {}", label);
        }
        Ok(())
    }

    fn cmd_stats(&self) -> Result<()> {
        let stats = self.get_stats();
        println!("Vertices: {}", stats.vertex_count);
        println!("Edges: {}", stats.edge_count);
        Ok(())
    }

    fn cmd_history<H: Helper>(
        &self,
        rl: &Editor<H, rustyline::history::DefaultHistory>,
    ) -> Result<()> {
        let history = rl.history();
        if history.is_empty() {
            println!("(no history)");
            return Ok(());
        }

        for (i, entry) in history.iter().enumerate() {
            println!("{:4}  {}", i + 1, entry);
        }
        Ok(())
    }

    fn cmd_clear(&self) -> Result<()> {
        print!("\x1B[2J\x1B[1;1H");
        std::io::stdout().flush().ok();
        Ok(())
    }

    fn cmd_quit(&self) -> Result<()> {
        println!("Goodbye!");
        std::process::exit(0);
    }

    fn cmd_set(&mut self, key: Option<&str>, value: Option<&str>) -> Result<()> {
        match (key, value) {
            (Some("format"), Some(v)) => {
                match v.to_lowercase().as_str() {
                    "table" => self.settings.format = OutputFormat::Table,
                    "json" => self.settings.format = OutputFormat::Json,
                    "csv" => self.settings.format = OutputFormat::Csv,
                    _ => eprintln!("Unknown format: {}. Use table, json, or csv", v),
                }
                println!("Format set to {:?}", self.settings.format);
            }
            (Some("limit"), Some(v)) => match v.parse::<usize>() {
                Ok(n) => {
                    self.settings.limit = n;
                    println!("Limit set to {}", n);
                }
                Err(_) => eprintln!("Invalid limit value: {}", v),
            },
            (Some("timing"), Some(v)) => match v.to_lowercase().as_str() {
                "true" | "on" | "1" | "yes" => {
                    self.settings.timing = true;
                    println!("Timing enabled");
                }
                "false" | "off" | "0" | "no" => {
                    self.settings.timing = false;
                    println!("Timing disabled");
                }
                _ => eprintln!("Invalid value: {}. Use true/false or on/off", v),
            },
            (Some(k), None) => {
                eprintln!("Missing value for setting '{}'", k);
            }
            (None, _) => {
                println!("Current settings:");
                println!("  format = {:?}", self.settings.format);
                println!("  limit = {}", self.settings.limit);
                println!("  timing = {}", self.settings.timing);
                println!();
                println!("Usage: .set <key> <value>");
            }
            _ => {}
        }
        Ok(())
    }

    fn cmd_read(&mut self, file: Option<&str>) -> Result<()> {
        let path = match file {
            Some(f) => PathBuf::from(f),
            None => {
                eprintln!("Usage: .read <file>");
                return Ok(());
            }
        };

        let content = fs::read_to_string(&path).map_err(|e| {
            CliError::io_with_source(format!("Failed to read file: {}", path.display()), e)
        })?;

        // Execute each line
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with("--") || line.starts_with("//") {
                continue;
            }

            println!("> {}", line);
            if let Err(e) = self.execute_query(line) {
                eprintln!("Error: {}", e);
            }
        }

        Ok(())
    }

    fn cmd_output(&mut self, file: Option<&str>) -> Result<()> {
        match file {
            Some("stdout") | None if self.settings.output_file.is_some() => {
                self.settings.output_file = None;
                println!("Output reset to stdout");
            }
            Some(f) if f != "stdout" => {
                self.settings.output_file = Some(PathBuf::from(f));
                println!("Output redirected to: {}", f);
            }
            None => {
                match &self.settings.output_file {
                    Some(f) => println!("Output: {}", f.display()),
                    None => println!("Output: stdout"),
                }
                println!("Usage: .output <file> or .output stdout");
            }
            _ => {}
        }
        Ok(())
    }

    /// Execute a query and display results.
    fn execute_query(&mut self, query: &str) -> Result<()> {
        let query = query.trim().trim_end_matches(';');
        if query.is_empty() {
            return Ok(());
        }

        let start = Instant::now();

        match self.mode {
            QueryMode::Gql => {
                let results = self.execute_gql(query)?;
                let elapsed = start.elapsed();

                let query_result = QueryResult::new(
                    results,
                    if self.settings.timing {
                        Some(elapsed)
                    } else {
                        None
                    },
                );

                self.output_results(&query_result)?;
            }
            QueryMode::Gremlin => {
                // Use execute_with_context to maintain variables across commands
                let context = std::mem::take(&mut self.gremlin_context);
                let script_result = self.gremlin_engine.execute_with_context(query, context)?;
                let elapsed = start.elapsed();

                // Update the context with any new variables
                self.gremlin_context = script_result.variables;

                // Format the Gremlin result (skip Unit results from assignments)
                if !matches!(script_result.result, ExecutionResult::Unit) {
                    let output = format_result(&script_result.result);

                    match &self.settings.output_file {
                        Some(path) => {
                            let mut file = File::create(path).map_err(|e| {
                                CliError::io_with_source(
                                    format!("Failed to open output file: {}", path.display()),
                                    e,
                                )
                            })?;
                            writeln!(file, "{}", output).map_err(|e| {
                                CliError::io_with_source("Failed to write to output file", e)
                            })?;
                        }
                        None => {
                            println!("{}", output);
                        }
                    }
                }

                if self.settings.timing {
                    println!("Time: {:.3}ms", elapsed.as_secs_f64() * 1000.0);
                }
            }
        }

        Ok(())
    }

    /// Output results to the configured destination.
    fn output_results(&self, result: &QueryResult) -> Result<()> {
        let output = format_results(result, self.settings.format, self.settings.limit);

        match &self.settings.output_file {
            Some(path) => {
                let mut file = File::create(path).map_err(|e| {
                    CliError::io_with_source(
                        format!("Failed to open output file: {}", path.display()),
                        e,
                    )
                })?;
                writeln!(file, "{}", output)
                    .map_err(|e| CliError::io_with_source("Failed to write to output file", e))?;
            }
            None => {
                println!("{}", output);
            }
        }

        Ok(())
    }

    // === Saved Query Commands ===

    fn cmd_queries(&self) -> Result<()> {
        let queries = self.graph().list_queries();
        if queries.is_empty() {
            println!("No saved queries.");
            println!("Use .save <name> <query> to save a query.");
        } else {
            println!(
                "{:<4} {:<20} {:<8} {}",
                "ID", "Name", "Type", "Description"
            );
            println!("{:-<4} {:-<20} {:-<8} {:-<40}", "", "", "", "");
            for q in &queries {
                let desc = if q.description.len() > 40 {
                    format!("{}...", &q.description[..37])
                } else {
                    q.description.clone()
                };
                println!("{:<4} {:<20} {:<8} {}", q.id, q.name, q.query_type, desc);
            }
            println!();
            println!("Total: {} queries", queries.len());
        }
        Ok(())
    }

    fn cmd_query(&self, name: Option<&str>) -> Result<()> {
        let name = match name {
            Some(n) => n,
            None => {
                eprintln!("Usage: .query <name>");
                return Ok(());
            }
        };

        match self.graph().get_query(name) {
            Some(q) => {
                println!("Name:        {}", q.name);
                println!("ID:          {}", q.id);
                println!("Type:        {}", q.query_type);
                if !q.description.is_empty() {
                    println!("Description: {}", q.description);
                }
                println!("Query:       {}", q.query);
                if !q.parameters.is_empty() {
                    let params: Vec<_> = q.parameters.iter().map(|p| format!("${}", p.name)).collect();
                    println!("Parameters:  {}", params.join(", "));
                }
            }
            None => {
                eprintln!("Query '{}' not found.", name);
            }
        }
        Ok(())
    }

    fn cmd_save(&mut self, name: Option<&str>, query_text: Option<&str>) -> Result<()> {
        use interstellar::query::QueryType;

        let name = match name {
            Some(n) => n,
            None => {
                eprintln!("Usage: .save <name> <query>");
                eprintln!("Example: .save find_people g.V().hasLabel('person').toList()");
                return Ok(());
            }
        };

        let query = match query_text {
            Some(q) => q.to_string(),
            None => {
                eprintln!("Usage: .save <name> <query>");
                eprintln!("Example: .save find_people g.V().hasLabel('person').toList()");
                return Ok(());
            }
        };

        let query_type = match self.mode {
            QueryMode::Gremlin => QueryType::Gremlin,
            QueryMode::Gql => QueryType::Gql,
        };

        match self.graph().save_query(name, query_type, "", &query) {
            Ok(id) => {
                // Checkpoint to persist
                if let Err(e) = self.graph().checkpoint() {
                    eprintln!("Warning: Failed to checkpoint: {}", e);
                }
                println!("Saved query '{}' (id: {}) as {:?}", name, id, query_type);
            }
            Err(e) => {
                eprintln!("Failed to save query: {}", e);
            }
        }
        Ok(())
    }

    fn cmd_delete_query(&self, name: Option<&str>) -> Result<()> {
        let name = match name {
            Some(n) => n,
            None => {
                eprintln!("Usage: .delete <name>");
                return Ok(());
            }
        };

        match self.graph().delete_query(name) {
            Ok(()) => {
                // Checkpoint to persist
                if let Err(e) = self.graph().checkpoint() {
                    eprintln!("Warning: Failed to checkpoint: {}", e);
                }
                println!("Deleted query '{}'", name);
            }
            Err(e) => {
                eprintln!("Failed to delete query: {}", e);
            }
        }
        Ok(())
    }

    fn cmd_run_query(&mut self, name: Option<&str>) -> Result<()> {
        use interstellar::query::QueryType;

        let name = match name {
            Some(n) => n,
            None => {
                eprintln!("Usage: .run <name>");
                return Ok(());
            }
        };

        let query = match self.graph().get_query(name) {
            Some(q) => q,
            None => {
                eprintln!("Query '{}' not found.", name);
                return Ok(());
            }
        };

        println!("Running: {}", query.query);

        // Switch mode temporarily if needed and execute
        let original_mode = self.mode;
        self.mode = match query.query_type {
            QueryType::Gremlin => QueryMode::Gremlin,
            QueryType::Gql => QueryMode::Gql,
        };

        let result = self.execute_query(&query.query);

        // Restore original mode
        self.mode = original_mode;

        result
    }
}
