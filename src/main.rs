use agui_reel::{analyze, render_svg, InputFormat, Report};
use clap::{Parser, Subcommand, ValueEnum};
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

#[derive(Debug, Parser)]
#[command(name = "agui-reel", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Validate an AG-UI stream and print a report.
    Check {
        /// JSONL/SSE input path, or - for stdin.
        #[arg(default_value = "-")]
        input: PathBuf,
        /// Input framing; auto detects common SSE prefixes.
        #[arg(long, value_enum, default_value_t = FormatArg::Auto)]
        format: FormatArg,
        /// Emit stable JSON rather than a human summary.
        #[arg(long)]
        json: bool,
    },
    /// Validate and render a standalone ordinal SVG timeline.
    Render {
        /// JSONL/SSE input path, or - for stdin.
        input: PathBuf,
        /// Destination SVG path.
        #[arg(short, long)]
        output: PathBuf,
        /// Input framing; auto detects common SSE prefixes.
        #[arg(long, value_enum, default_value_t = FormatArg::Auto)]
        format: FormatArg,
        /// Replace an existing destination.
        #[arg(long)]
        force: bool,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum, Default)]
enum FormatArg {
    #[default]
    Auto,
    Jsonl,
    Sse,
}

impl From<FormatArg> for InputFormat {
    fn from(value: FormatArg) -> Self {
        match value {
            FormatArg::Auto => Self::Auto,
            FormatArg::Jsonl => Self::Jsonl,
            FormatArg::Sse => Self::Sse,
        }
    }
}

fn main() -> ExitCode {
    match run(Cli::parse()) {
        Ok(valid) if valid => ExitCode::SUCCESS,
        Ok(_) => ExitCode::from(2),
        Err(error) => {
            eprintln!("agui-reel: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run(cli: Cli) -> Result<bool, Box<dyn std::error::Error>> {
    match cli.command {
        Command::Check {
            input,
            format,
            json,
        } => {
            let content = read_input(&input)?;
            let analysis = analyze(&content, format.into())?;
            if json {
                println!("{}", serde_json::to_string_pretty(&analysis.report)?);
            } else {
                print_human(&analysis.report);
            }
            Ok(analysis.report.valid)
        }
        Command::Render {
            input,
            output,
            format,
            force,
        } => {
            let content = read_input(&input)?;
            let analysis = analyze(&content, format.into())?;
            if !analysis.report.valid {
                print_human(&analysis.report);
                return Ok(false);
            }
            let svg = render_svg(&analysis);
            write_output(&output, svg.as_bytes(), force)?;
            println!(
                "rendered {} events to {}",
                analysis.report.event_count,
                output.display()
            );
            Ok(true)
        }
    }
}

fn read_input(path: &Path) -> io::Result<String> {
    if path == Path::new("-") {
        let mut content = String::new();
        io::stdin().read_to_string(&mut content)?;
        Ok(content)
    } else {
        fs::read_to_string(path)
    }
}

fn write_output(path: &Path, bytes: &[u8], force: bool) -> io::Result<()> {
    let mut options = OpenOptions::new();
    options.write(true);
    if force {
        options.create(true).truncate(true);
    } else {
        options.create_new(true);
    }
    let mut file = options.open(path)?;
    file.write_all(bytes)
}

fn print_human(report: &Report) {
    println!(
        "{}: {} events, {} subagents, depth {}, sha256 {}",
        if report.valid { "VALID" } else { "INVALID" },
        report.event_count,
        report.subagent_count,
        report.max_subagent_depth,
        report.sha256
    );
    if let (Some(thread), Some(run)) = (&report.thread_id, &report.run_id) {
        println!("run: {thread}/{run}");
    }
    for issue in &report.issues {
        match issue.event {
            Some(event) => println!("- [{}] event #{event}: {}", issue.code, issue.message),
            None => println!("- [{}] {}", issue.code, issue.message),
        }
    }
}
