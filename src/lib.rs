//! Semantic validation and ordinal timeline rendering for AG-UI 1.0 streams.
//!
//! AG-UI declares arrival order as protocol order. `agui-reel` therefore never
//! sorts events by their optional timestamps and renders an ordinal timeline.

use serde::Serialize;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};
use thiserror::Error;

const EVENT_TYPES: &[&str] = &[
    "TEXT_MESSAGE_START",
    "TEXT_MESSAGE_CONTENT",
    "TEXT_MESSAGE_END",
    "TEXT_MESSAGE_CHUNK",
    "TOOL_CALL_START",
    "TOOL_CALL_ARGS",
    "TOOL_CALL_END",
    "TOOL_CALL_CHUNK",
    "TOOL_CALL_RESULT",
    "STATE_SNAPSHOT",
    "STATE_DELTA",
    "MESSAGES_SNAPSHOT",
    "ACTIVITY_SNAPSHOT",
    "ACTIVITY_DELTA",
    "RAW",
    "CUSTOM",
    "RUN_STARTED",
    "RUN_FINISHED",
    "RUN_ERROR",
    "STEP_STARTED",
    "STEP_FINISHED",
    "REASONING_START",
    "REASONING_MESSAGE_START",
    "REASONING_MESSAGE_CONTENT",
    "REASONING_MESSAGE_END",
    "REASONING_MESSAGE_CHUNK",
    "REASONING_END",
    "REASONING_ENCRYPTED_VALUE",
    "SUBAGENT_STARTED",
    "SUBAGENT_FINISHED",
    "SUBAGENT_ERROR",
];

/// Input framing accepted by [`analyze`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputFormat {
    /// Detect SSE from its field prefixes, otherwise use JSON Lines.
    Auto,
    /// One JSON event object per non-empty line.
    Jsonl,
    /// Server-Sent Events, with JSON in one or more `data:` fields.
    Sse,
}

impl InputFormat {
    fn label(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Jsonl => "jsonl",
            Self::Sse => "sse",
        }
    }
}

/// A semantic problem found in an otherwise parseable stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Issue {
    /// Stable machine-readable identifier.
    pub code: String,
    /// One-based event ordinal, or `None` for a whole-stream issue.
    pub event: Option<usize>,
    /// Human-readable explanation.
    pub message: String,
}

/// Summary returned by the validator and emitted by `agui-reel check --json`.
#[derive(Debug, Clone, Serialize)]
pub struct Report {
    pub valid: bool,
    pub format: String,
    pub sha256: String,
    pub event_count: usize,
    pub event_types: BTreeMap<String, usize>,
    pub thread_id: Option<String>,
    pub run_id: Option<String>,
    pub terminal_event: Option<String>,
    pub subagent_count: usize,
    pub max_subagent_depth: usize,
    pub issues: Vec<Issue>,
}

/// Parsed events plus their validation report.
#[derive(Debug, Clone)]
pub struct Analysis {
    pub report: Report,
    events: Vec<Event>,
}

#[derive(Debug, Clone)]
struct Event {
    ordinal: usize,
    kind: String,
    object: Map<String, Value>,
}

impl Event {
    fn text(&self, field: &str) -> Option<&str> {
        self.object.get(field).and_then(Value::as_str)
    }

    fn lane(&self) -> Option<&str> {
        match self.kind.as_str() {
            "SUBAGENT_STARTED" => self.text("parentSubagentRunId"),
            "SUBAGENT_FINISHED" | "SUBAGENT_ERROR" => self.text("subagentRunId"),
            _ => self.text("subagentRunId"),
        }
    }
}

/// Framing or JSON failure. Semantic failures are returned in [`Report::issues`].
#[derive(Debug, Error)]
pub enum ParseError {
    #[error("line {line}: expected a JSON object")]
    ExpectedObject { line: usize },
    #[error("line {line}: missing string field `type`")]
    MissingType { line: usize },
    #[error("line {line}: invalid JSON: {source}")]
    InvalidJson {
        line: usize,
        #[source]
        source: serde_json::Error,
    },
    #[error("line {line}: SSE data block is empty")]
    EmptySseData { line: usize },
}

/// Parse and semantically validate a complete AG-UI event stream.
pub fn analyze(input: &str, requested: InputFormat) -> Result<Analysis, ParseError> {
    let format = match requested {
        InputFormat::Auto => detect_format(input),
        explicit => explicit,
    };
    let events = match format {
        InputFormat::Sse => parse_sse(input)?,
        InputFormat::Jsonl | InputFormat::Auto => parse_jsonl(input)?,
    };
    let sha256 = format!("{:x}", Sha256::digest(input.as_bytes()));
    let report = validate(&events, format, sha256);
    Ok(Analysis { report, events })
}

fn detect_format(input: &str) -> InputFormat {
    for line in input.lines() {
        let line = line.trim_start();
        if line.is_empty() {
            continue;
        }
        if line.starts_with("data:")
            || line.starts_with("event:")
            || line.starts_with("id:")
            || line.starts_with("retry:")
            || line.starts_with(':')
        {
            return InputFormat::Sse;
        }
        break;
    }
    InputFormat::Jsonl
}

fn parse_jsonl(input: &str) -> Result<Vec<Event>, ParseError> {
    let mut events = Vec::new();
    for (line_index, line) in input.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        events.push(parse_event(line, line_index + 1, events.len() + 1)?);
    }
    Ok(events)
}

fn parse_sse(input: &str) -> Result<Vec<Event>, ParseError> {
    let mut events = Vec::new();
    let mut data = Vec::new();
    let mut block_line = 1;

    let flush = |data: &mut Vec<String>,
                 block_line: usize,
                 events: &mut Vec<Event>|
     -> Result<(), ParseError> {
        if data.is_empty() {
            return Ok(());
        }
        let payload = data.join("\n");
        if payload.trim().is_empty() {
            return Err(ParseError::EmptySseData { line: block_line });
        }
        events.push(parse_event(&payload, block_line, events.len() + 1)?);
        data.clear();
        Ok(())
    };

    for (line_index, line) in input.lines().enumerate() {
        let number = line_index + 1;
        if line.is_empty() {
            flush(&mut data, block_line, &mut events)?;
            block_line = number + 1;
            continue;
        }
        if line.starts_with(':') {
            continue;
        }
        if let Some(value) = line.strip_prefix("data:") {
            data.push(value.strip_prefix(' ').unwrap_or(value).to_owned());
        }
    }
    flush(&mut data, block_line, &mut events)?;
    Ok(events)
}

fn parse_event(payload: &str, line: usize, ordinal: usize) -> Result<Event, ParseError> {
    let value: Value =
        serde_json::from_str(payload).map_err(|source| ParseError::InvalidJson { line, source })?;
    let object = value
        .as_object()
        .cloned()
        .ok_or(ParseError::ExpectedObject { line })?;
    let kind = object
        .get("type")
        .and_then(Value::as_str)
        .ok_or(ParseError::MissingType { line })?
        .to_owned();
    Ok(Event {
        ordinal,
        kind,
        object,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SubagentState {
    Active,
    Closed,
}

fn validate(events: &[Event], format: InputFormat, sha256: String) -> Report {
    let mut issues = Vec::new();
    let mut counts = BTreeMap::new();
    let mut started = false;
    let mut terminal = false;
    let mut thread_id = None;
    let mut run_id = None;
    let mut terminal_event = None;

    let mut seen_messages = HashSet::new();
    let mut open_messages: HashMap<String, Option<String>> = HashMap::new();
    let mut seen_tools = HashSet::new();
    let mut open_tools: HashMap<String, Option<String>> = HashMap::new();
    let mut completed_tools: HashMap<String, Option<String>> = HashMap::new();
    let mut tool_results = HashSet::new();
    let mut open_steps = HashSet::new();
    let mut subagents: HashMap<String, (SubagentState, Option<String>, usize)> = HashMap::new();
    let mut max_depth = 0;
    let mut chunk_message_by_lane: HashMap<Option<String>, String> = HashMap::new();
    let mut chunk_tool_by_lane: HashMap<Option<String>, String> = HashMap::new();

    for event in events {
        *counts.entry(event.kind.clone()).or_insert(0) += 1;
        validate_shape(event, &mut issues);

        if !EVENT_TYPES.contains(&event.kind.as_str()) {
            push_issue(
                &mut issues,
                "unknown_event_type",
                Some(event.ordinal),
                format!("`{}` is not an AG-UI 1.0 event type", event.kind),
            );
        }

        if event.kind == "RUN_STARTED" {
            if started {
                push_issue(
                    &mut issues,
                    "duplicate_run_start",
                    Some(event.ordinal),
                    "a stream may contain exactly one RUN_STARTED",
                );
            }
            if event.ordinal != 1 {
                push_issue(
                    &mut issues,
                    "run_start_not_first",
                    Some(event.ordinal),
                    "RUN_STARTED must be the first event",
                );
            }
            started = true;
            thread_id = event.text("threadId").map(str::to_owned);
            run_id = event.text("runId").map(str::to_owned);
            continue;
        }

        if !started {
            push_issue(
                &mut issues,
                "event_before_run",
                Some(event.ordinal),
                "event appears before RUN_STARTED",
            );
            continue;
        }
        if terminal {
            push_issue(
                &mut issues,
                "event_after_terminal",
                Some(event.ordinal),
                "no event may follow RUN_FINISHED or RUN_ERROR",
            );
            continue;
        }

        if let Some(lane) = attributable_lane(event) {
            match subagents.get(lane) {
                Some((SubagentState::Active, _, _)) => {}
                Some((SubagentState::Closed, _, _)) => push_issue(
                    &mut issues,
                    "event_for_closed_subagent",
                    Some(event.ordinal),
                    format!("event is attributed to closed subagent `{lane}`"),
                ),
                None => push_issue(
                    &mut issues,
                    "unknown_subagent",
                    Some(event.ordinal),
                    format!("event is attributed to unknown subagent `{lane}`"),
                ),
            }
        }

        let lane = event.text("subagentRunId").map(str::to_owned);
        match event.kind.as_str() {
            "RUN_FINISHED" => {
                if event.text("threadId") != thread_id.as_deref() {
                    push_issue(
                        &mut issues,
                        "thread_id_mismatch",
                        Some(event.ordinal),
                        "RUN_FINISHED threadId does not match RUN_STARTED",
                    );
                }
                if event.text("runId") != run_id.as_deref() {
                    push_issue(
                        &mut issues,
                        "run_id_mismatch",
                        Some(event.ordinal),
                        "RUN_FINISHED runId does not match RUN_STARTED",
                    );
                }
                terminal = true;
                terminal_event = Some(event.kind.clone());
            }
            "RUN_ERROR" => {
                terminal = true;
                terminal_event = Some(event.kind.clone());
            }
            "TEXT_MESSAGE_START" => {
                if let Some(id) = event.text("messageId") {
                    if !seen_messages.insert(id.to_owned()) {
                        push_issue(
                            &mut issues,
                            "duplicate_message_id",
                            Some(event.ordinal),
                            format!("messageId `{id}` was already used"),
                        );
                    } else {
                        open_messages.insert(id.to_owned(), lane);
                    }
                }
            }
            "TEXT_MESSAGE_CONTENT" => {
                if let Some(id) = event.text("messageId") {
                    require_open_lane(
                        &open_messages,
                        id,
                        lane.as_deref(),
                        event,
                        "message",
                        &mut issues,
                    );
                }
            }
            "TEXT_MESSAGE_END" => {
                if let Some(id) = event.text("messageId") {
                    if require_open_lane(
                        &open_messages,
                        id,
                        lane.as_deref(),
                        event,
                        "message",
                        &mut issues,
                    ) {
                        open_messages.remove(id);
                    }
                }
            }
            "TEXT_MESSAGE_CHUNK" => validate_chunk(
                event,
                lane,
                "messageId",
                "message",
                &mut chunk_message_by_lane,
                &mut issues,
            ),
            "TOOL_CALL_START" => {
                if let Some(id) = event.text("toolCallId") {
                    if !seen_tools.insert(id.to_owned()) {
                        push_issue(
                            &mut issues,
                            "duplicate_tool_call_id",
                            Some(event.ordinal),
                            format!("toolCallId `{id}` was already used"),
                        );
                    } else {
                        open_tools.insert(id.to_owned(), lane);
                    }
                }
            }
            "TOOL_CALL_ARGS" => {
                if let Some(id) = event.text("toolCallId") {
                    require_open_lane(
                        &open_tools,
                        id,
                        lane.as_deref(),
                        event,
                        "tool call",
                        &mut issues,
                    );
                }
            }
            "TOOL_CALL_END" => {
                if let Some(id) = event.text("toolCallId") {
                    if require_open_lane(
                        &open_tools,
                        id,
                        lane.as_deref(),
                        event,
                        "tool call",
                        &mut issues,
                    ) {
                        open_tools.remove(id);
                        completed_tools.insert(id.to_owned(), lane);
                    }
                }
            }
            "TOOL_CALL_CHUNK" => validate_chunk(
                event,
                lane,
                "toolCallId",
                "tool call",
                &mut chunk_tool_by_lane,
                &mut issues,
            ),
            "TOOL_CALL_RESULT" => {
                if let Some(id) = event.text("toolCallId") {
                    require_completed_tool(
                        &completed_tools,
                        id,
                        lane.as_deref(),
                        event,
                        &mut issues,
                    );
                    if !tool_results.insert(id.to_owned()) {
                        push_issue(
                            &mut issues,
                            "duplicate_tool_result",
                            Some(event.ordinal),
                            format!("tool call `{id}` has more than one result"),
                        );
                    }
                }
                if let Some(message_id) = event.text("messageId") {
                    if !seen_messages.insert(message_id.to_owned()) {
                        push_issue(
                            &mut issues,
                            "duplicate_message_id",
                            Some(event.ordinal),
                            format!("messageId `{message_id}` was already used"),
                        );
                    }
                }
            }
            "STEP_STARTED" => {
                if let Some(name) = event.text("stepName") {
                    let key = lane_key(lane.as_deref(), name);
                    if !open_steps.insert(key) {
                        push_issue(
                            &mut issues,
                            "duplicate_open_step",
                            Some(event.ordinal),
                            format!("step `{name}` is already open in this lane"),
                        );
                    }
                }
            }
            "STEP_FINISHED" => {
                if let Some(name) = event.text("stepName") {
                    let key = lane_key(lane.as_deref(), name);
                    if !open_steps.remove(&key) {
                        push_issue(
                            &mut issues,
                            "step_not_open",
                            Some(event.ordinal),
                            format!("step `{name}` is not open in this lane"),
                        );
                    }
                }
            }
            "SUBAGENT_STARTED" => {
                if let Some(id) = event.text("subagentRunId") {
                    let parent = event.text("parentSubagentRunId").map(str::to_owned);
                    let depth = match parent.as_deref() {
                        None => 1,
                        Some(parent_id) => match subagents.get(parent_id) {
                            Some((SubagentState::Active, _, depth)) => depth + 1,
                            Some((SubagentState::Closed, _, _)) => {
                                push_issue(
                                    &mut issues,
                                    "closed_subagent_parent",
                                    Some(event.ordinal),
                                    format!("parent subagent `{parent_id}` is closed"),
                                );
                                1
                            }
                            None => {
                                push_issue(
                                    &mut issues,
                                    "unknown_subagent_parent",
                                    Some(event.ordinal),
                                    format!("parent subagent `{parent_id}` is unknown"),
                                );
                                1
                            }
                        },
                    };
                    max_depth = max_depth.max(depth);
                    if subagents
                        .insert(id.to_owned(), (SubagentState::Active, parent, depth))
                        .is_some()
                    {
                        push_issue(
                            &mut issues,
                            "duplicate_subagent_id",
                            Some(event.ordinal),
                            format!("subagentRunId `{id}` was already used"),
                        );
                    }
                }
            }
            "SUBAGENT_FINISHED" | "SUBAGENT_ERROR" => {
                if let Some(id) = event.text("subagentRunId") {
                    if let Some(child) =
                        subagents.iter().find_map(|(child_id, (state, parent, _))| {
                            (*state == SubagentState::Active && parent.as_deref() == Some(id))
                                .then_some(child_id)
                        })
                    {
                        push_issue(
                            &mut issues,
                            "active_child_subagent",
                            Some(event.ordinal),
                            format!("subagent `{id}` cannot close while child `{child}` is active"),
                        );
                    }
                    match subagents.get_mut(id) {
                        Some((state @ SubagentState::Active, _, _)) => {
                            *state = SubagentState::Closed;
                        }
                        Some((SubagentState::Closed, _, _)) => push_issue(
                            &mut issues,
                            "subagent_already_closed",
                            Some(event.ordinal),
                            format!("subagent `{id}` is already closed"),
                        ),
                        None => push_issue(
                            &mut issues,
                            "unknown_subagent",
                            Some(event.ordinal),
                            format!("cannot close unknown subagent `{id}`"),
                        ),
                    }
                }
            }
            _ => {}
        }
    }

    if !started {
        push_issue(
            &mut issues,
            "missing_run_start",
            None,
            "stream has no RUN_STARTED event",
        );
    }
    if !terminal {
        push_issue(
            &mut issues,
            "missing_terminal_event",
            None,
            "stream has no RUN_FINISHED or RUN_ERROR event",
        );
    }
    for id in sorted_keys(&open_messages) {
        push_issue(
            &mut issues,
            "unclosed_message",
            None,
            format!("message `{id}` has no TEXT_MESSAGE_END"),
        );
    }
    for id in sorted_keys(&open_tools) {
        push_issue(
            &mut issues,
            "unclosed_tool_call",
            None,
            format!("tool call `{id}` has no TOOL_CALL_END"),
        );
    }
    let mut steps: Vec<_> = open_steps.into_iter().collect();
    steps.sort();
    for step in steps {
        push_issue(
            &mut issues,
            "unclosed_step",
            None,
            format!("step `{step}` has no STEP_FINISHED"),
        );
    }
    let mut active_subagents: Vec<_> = subagents
        .iter()
        .filter(|(_, (state, _, _))| *state == SubagentState::Active)
        .map(|(id, _)| id.clone())
        .collect();
    active_subagents.sort();
    for id in active_subagents {
        push_issue(
            &mut issues,
            "unclosed_subagent",
            None,
            format!("subagent `{id}` has no terminal event"),
        );
    }

    Report {
        valid: issues.is_empty(),
        format: format.label().to_owned(),
        sha256,
        event_count: events.len(),
        event_types: counts,
        thread_id,
        run_id,
        terminal_event,
        subagent_count: subagents.len(),
        max_subagent_depth: max_depth,
        issues,
    }
}

fn validate_shape(event: &Event, issues: &mut Vec<Issue>) {
    let required: &[&str] = match event.kind.as_str() {
        "RUN_STARTED" | "RUN_FINISHED" => &["threadId", "runId"],
        "RUN_ERROR" => &["message"],
        "TEXT_MESSAGE_START" | "TEXT_MESSAGE_END" => &["messageId"],
        "TEXT_MESSAGE_CONTENT" => &["messageId", "delta"],
        "TOOL_CALL_START" => &["toolCallId", "toolCallName"],
        "TOOL_CALL_ARGS" => &["toolCallId", "delta"],
        "TOOL_CALL_END" => &["toolCallId"],
        "TOOL_CALL_RESULT" => &["messageId", "toolCallId"],
        "STEP_STARTED" | "STEP_FINISHED" => &["stepName"],
        "SUBAGENT_STARTED" => &["subagentRunId", "name"],
        "SUBAGENT_FINISHED" => &["subagentRunId"],
        "SUBAGENT_ERROR" => &["subagentRunId", "message"],
        "REASONING_MESSAGE_START" | "REASONING_MESSAGE_END" => &["messageId"],
        "REASONING_MESSAGE_CONTENT" => &["messageId", "delta"],
        _ => &[],
    };
    for field in required {
        if event.text(field).is_none() {
            push_issue(
                issues,
                "missing_string_field",
                Some(event.ordinal),
                format!("{} requires string field `{field}`", event.kind),
            );
        }
    }
    if event.kind == "TOOL_CALL_RESULT" && event.object.get("content").is_none_or(Value::is_null) {
        push_issue(
            issues,
            "missing_content",
            Some(event.ordinal),
            "TOOL_CALL_RESULT requires non-null `content`",
        );
    }
}

fn attributable_lane(event: &Event) -> Option<&str> {
    match event.kind.as_str() {
        "RUN_STARTED" | "RUN_FINISHED" | "RUN_ERROR" | "MESSAGES_SNAPSHOT" | "SUBAGENT_STARTED"
        | "SUBAGENT_FINISHED" | "SUBAGENT_ERROR" => None,
        _ => event.text("subagentRunId"),
    }
}

fn require_open_lane(
    open: &HashMap<String, Option<String>>,
    id: &str,
    lane: Option<&str>,
    event: &Event,
    label: &str,
    issues: &mut Vec<Issue>,
) -> bool {
    match open.get(id) {
        None => {
            push_issue(
                issues,
                format!("{label}_not_open").replace(' ', "_"),
                Some(event.ordinal),
                format!("{label} `{id}` is not open"),
            );
            false
        }
        Some(expected) if expected.as_deref() != lane => {
            push_issue(
                issues,
                "lineage_mismatch",
                Some(event.ordinal),
                format!("{label} `{id}` changed subagent attribution"),
            );
            false
        }
        Some(_) => true,
    }
}

fn require_completed_tool(
    completed: &HashMap<String, Option<String>>,
    id: &str,
    lane: Option<&str>,
    event: &Event,
    issues: &mut Vec<Issue>,
) -> bool {
    match completed.get(id) {
        None => {
            push_issue(
                issues,
                "result_before_tool_end",
                Some(event.ordinal),
                format!("result for `{id}` appears before its TOOL_CALL_END"),
            );
            false
        }
        Some(expected) if expected.as_deref() != lane => {
            push_issue(
                issues,
                "lineage_mismatch",
                Some(event.ordinal),
                format!("tool result `{id}` changed subagent attribution"),
            );
            false
        }
        Some(_) => true,
    }
}

fn validate_chunk(
    event: &Event,
    lane: Option<String>,
    id_field: &str,
    label: &str,
    current: &mut HashMap<Option<String>, String>,
    issues: &mut Vec<Issue>,
) {
    if let Some(id) = event.text(id_field) {
        current.insert(lane, id.to_owned());
    } else if !current.contains_key(&lane) {
        push_issue(
            issues,
            "orphan_chunk",
            Some(event.ordinal),
            format!("{label} chunk has no `{id_field}` and no prior chunk in its lane"),
        );
    }
}

fn lane_key(lane: Option<&str>, value: &str) -> String {
    format!("{}::{value}", lane.unwrap_or("root"))
}

fn sorted_keys(map: &HashMap<String, Option<String>>) -> Vec<String> {
    let mut keys: Vec<_> = map.keys().cloned().collect();
    keys.sort();
    keys
}

fn push_issue(
    issues: &mut Vec<Issue>,
    code: impl Into<String>,
    event: Option<usize>,
    message: impl Into<String>,
) {
    issues.push(Issue {
        code: code.into(),
        event,
        message: message.into(),
    });
}

/// Render a standalone SVG timeline using event ordinal as the horizontal axis.
pub fn render_svg(analysis: &Analysis) -> String {
    let mut lane_order = vec!["root".to_owned()];
    for event in &analysis.events {
        if event.kind == "SUBAGENT_STARTED" {
            if let Some(id) = event.text("subagentRunId") {
                if !lane_order.iter().any(|lane| lane == id) {
                    lane_order.push(id.to_owned());
                }
            }
        }
    }
    let width = (280 + analysis.events.len() * 34).max(960);
    let height = 155 + lane_order.len() * 84;
    let chart_left = 190usize;
    let chart_width = width - chart_left - 35;
    let step = if analysis.events.len() <= 1 {
        0.0
    } else {
        chart_width as f64 / (analysis.events.len() - 1) as f64
    };
    let issue_events: HashSet<_> = analysis
        .report
        .issues
        .iter()
        .filter_map(|issue| issue.event)
        .collect();

    let mut out = String::new();
    out.push_str(&format!(
        "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"{width}\" height=\"{height}\" viewBox=\"0 0 {width} {height}\">\n"
    ));
    out.push_str("<style>text{font-family:ui-monospace,SFMono-Regular,Menlo,monospace}.muted{fill:#64748b}.lane{stroke:#cbd5e1;stroke-width:2}.tick{stroke:#e2e8f0}.label{font-size:12px}.title{font-size:20px;font-weight:700}</style>\n");
    out.push_str("<rect width=\"100%\" height=\"100%\" fill=\"#f8fafc\"/>\n");
    out.push_str(&format!(
        "<text x=\"24\" y=\"34\" class=\"title\">AG-UI Reel — {}</text>\n",
        if analysis.report.valid {
            "valid"
        } else {
            "invalid"
        }
    ));
    out.push_str(&format!(
        "<text x=\"24\" y=\"57\" class=\"muted label\">{} events · SHA-256 {}</text>\n",
        analysis.report.event_count,
        &analysis.report.sha256[..12]
    ));

    for (lane_index, lane) in lane_order.iter().enumerate() {
        let y = 105 + lane_index * 84;
        out.push_str(&format!(
            "<text x=\"24\" y=\"{}\" class=\"label\">{}</text>\n",
            y + 4,
            escape_xml(lane)
        ));
        out.push_str(&format!(
            "<line x1=\"{chart_left}\" y1=\"{y}\" x2=\"{}\" y2=\"{y}\" class=\"lane\"/>\n",
            width - 35
        ));
    }

    for event in &analysis.events {
        let x = chart_left as f64 + (event.ordinal.saturating_sub(1)) as f64 * step;
        let lane = event.lane().unwrap_or("root");
        let lane_index = lane_order.iter().position(|item| item == lane).unwrap_or(0);
        let y = 105 + lane_index * 84;
        let color = event_color(&event.kind);
        let invalid = issue_events.contains(&event.ordinal);
        let stroke = if invalid { "#dc2626" } else { "#ffffff" };
        let radius = if invalid { 9 } else { 7 };
        out.push_str(&format!(
            "<line x1=\"{x:.1}\" y1=\"78\" x2=\"{x:.1}\" y2=\"{}\" class=\"tick\"/>\n",
            height - 40
        ));
        out.push_str(&format!(
            "<circle cx=\"{x:.1}\" cy=\"{y}\" r=\"{radius}\" fill=\"{color}\" stroke=\"{stroke}\" stroke-width=\"3\"><title>#{}: {}</title></circle>\n",
            event.ordinal,
            escape_xml(&event.kind)
        ));
        out.push_str(&format!(
            "<text x=\"{x:.1}\" y=\"{}\" class=\"muted label\" text-anchor=\"middle\">{}</text>\n",
            y + 25,
            event.ordinal
        ));
    }
    out.push_str("<text x=\"24\" y=\"100%\" dy=\"-18\" class=\"muted label\">arrival order → · red outline = diagnostic</text>\n</svg>\n");
    out
}

fn event_color(kind: &str) -> &'static str {
    if kind.starts_with("RUN_") {
        "#7c3aed"
    } else if kind.starts_with("SUBAGENT_") {
        "#db2777"
    } else if kind.starts_with("TOOL_CALL_") {
        "#ea580c"
    } else if kind.starts_with("TEXT_MESSAGE_") || kind.starts_with("REASONING_") {
        "#2563eb"
    } else if kind.starts_with("STEP_") {
        "#059669"
    } else {
        "#475569"
    }
}

fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID: &str = concat!(
        "{\"type\":\"RUN_STARTED\",\"threadId\":\"t1\",\"runId\":\"r1\",\"protocolVersion\":\"1.0\"}\n",
        "{\"type\":\"TEXT_MESSAGE_START\",\"messageId\":\"m1\"}\n",
        "{\"type\":\"TEXT_MESSAGE_CONTENT\",\"messageId\":\"m1\",\"delta\":\"hello\"}\n",
        "{\"type\":\"TEXT_MESSAGE_END\",\"messageId\":\"m1\"}\n",
        "{\"type\":\"RUN_FINISHED\",\"threadId\":\"t1\",\"runId\":\"r1\"}\n"
    );

    #[test]
    fn accepts_complete_jsonl_run() {
        let analysis = analyze(VALID, InputFormat::Auto).expect("parse");
        assert!(analysis.report.valid, "{:?}", analysis.report.issues);
        assert_eq!(analysis.report.event_count, 5);
        assert_eq!(
            analysis.report.terminal_event.as_deref(),
            Some("RUN_FINISHED")
        );
    }

    #[test]
    fn accepts_sse_and_ignores_metadata() {
        let sse = VALID
            .lines()
            .enumerate()
            .map(|(index, line)| format!("id: {index}\nevent: message\ndata: {line}\n\n"))
            .collect::<String>();
        let analysis = analyze(&sse, InputFormat::Auto).expect("parse");
        assert!(analysis.report.valid, "{:?}", analysis.report.issues);
        assert_eq!(analysis.report.format, "sse");
    }

    #[test]
    fn rejects_missing_message_end_and_terminal() {
        let input = concat!(
            "{\"type\":\"RUN_STARTED\",\"threadId\":\"t\",\"runId\":\"r\"}\n",
            "{\"type\":\"TEXT_MESSAGE_START\",\"messageId\":\"m\"}\n"
        );
        let report = analyze(input, InputFormat::Jsonl).expect("parse").report;
        assert!(!report.valid);
        let codes: HashSet<_> = report
            .issues
            .iter()
            .map(|issue| issue.code.as_str())
            .collect();
        assert!(codes.contains("missing_terminal_event"));
        assert!(codes.contains("unclosed_message"));
    }

    #[test]
    fn rejects_lineage_switch_mid_tool_call() {
        let input = concat!(
            "{\"type\":\"RUN_STARTED\",\"threadId\":\"t\",\"runId\":\"r\"}\n",
            "{\"type\":\"SUBAGENT_STARTED\",\"subagentRunId\":\"s1\",\"name\":\"worker\"}\n",
            "{\"type\":\"TOOL_CALL_START\",\"toolCallId\":\"c1\",\"toolCallName\":\"read\",\"subagentRunId\":\"s1\"}\n",
            "{\"type\":\"TOOL_CALL_END\",\"toolCallId\":\"c1\"}\n",
            "{\"type\":\"SUBAGENT_FINISHED\",\"subagentRunId\":\"s1\"}\n",
            "{\"type\":\"RUN_FINISHED\",\"threadId\":\"t\",\"runId\":\"r\"}\n"
        );
        let report = analyze(input, InputFormat::Jsonl).expect("parse").report;
        assert!(report
            .issues
            .iter()
            .any(|issue| issue.code == "lineage_mismatch"));
    }

    #[test]
    fn nested_subagents_produce_depth_and_svg_lanes() {
        let input = concat!(
            "{\"type\":\"RUN_STARTED\",\"threadId\":\"t\",\"runId\":\"r\"}\n",
            "{\"type\":\"SUBAGENT_STARTED\",\"subagentRunId\":\"s1\",\"name\":\"lead\"}\n",
            "{\"type\":\"SUBAGENT_STARTED\",\"subagentRunId\":\"s2\",\"name\":\"worker\",\"parentSubagentRunId\":\"s1\"}\n",
            "{\"type\":\"SUBAGENT_FINISHED\",\"subagentRunId\":\"s2\"}\n",
            "{\"type\":\"SUBAGENT_FINISHED\",\"subagentRunId\":\"s1\"}\n",
            "{\"type\":\"RUN_FINISHED\",\"threadId\":\"t\",\"runId\":\"r\"}\n"
        );
        let analysis = analyze(input, InputFormat::Jsonl).expect("parse");
        assert!(analysis.report.valid, "{:?}", analysis.report.issues);
        assert_eq!(analysis.report.max_subagent_depth, 2);
        let svg = render_svg(&analysis);
        assert!(svg.contains("s1"));
        assert!(svg.contains("s2"));
    }

    #[test]
    fn tool_result_cannot_switch_lineage() {
        let input = concat!(
            "{\"type\":\"RUN_STARTED\",\"threadId\":\"t\",\"runId\":\"r\"}\n",
            "{\"type\":\"SUBAGENT_STARTED\",\"subagentRunId\":\"s1\",\"name\":\"worker\"}\n",
            "{\"type\":\"TOOL_CALL_START\",\"toolCallId\":\"c1\",\"toolCallName\":\"read\",\"subagentRunId\":\"s1\"}\n",
            "{\"type\":\"TOOL_CALL_END\",\"toolCallId\":\"c1\",\"subagentRunId\":\"s1\"}\n",
            "{\"type\":\"TOOL_CALL_RESULT\",\"toolCallId\":\"c1\",\"messageId\":\"m1\",\"content\":\"ok\"}\n",
            "{\"type\":\"SUBAGENT_FINISHED\",\"subagentRunId\":\"s1\"}\n",
            "{\"type\":\"RUN_FINISHED\",\"threadId\":\"t\",\"runId\":\"r\"}\n"
        );
        let report = analyze(input, InputFormat::Jsonl).expect("parse").report;
        assert!(report
            .issues
            .iter()
            .any(|issue| issue.code == "lineage_mismatch"));
    }

    #[test]
    fn parent_cannot_finish_before_child() {
        let input = concat!(
            "{\"type\":\"RUN_STARTED\",\"threadId\":\"t\",\"runId\":\"r\"}\n",
            "{\"type\":\"SUBAGENT_STARTED\",\"subagentRunId\":\"s1\",\"name\":\"lead\"}\n",
            "{\"type\":\"SUBAGENT_STARTED\",\"subagentRunId\":\"s2\",\"name\":\"worker\",\"parentSubagentRunId\":\"s1\"}\n",
            "{\"type\":\"SUBAGENT_FINISHED\",\"subagentRunId\":\"s1\"}\n",
            "{\"type\":\"SUBAGENT_FINISHED\",\"subagentRunId\":\"s2\"}\n",
            "{\"type\":\"RUN_FINISHED\",\"threadId\":\"t\",\"runId\":\"r\"}\n"
        );
        let report = analyze(input, InputFormat::Jsonl).expect("parse").report;
        assert!(report
            .issues
            .iter()
            .any(|issue| issue.code == "active_child_subagent"));
    }
}
