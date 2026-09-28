//! A turn as the owner reads it, whichever client shows it: the phone
//! contract ([`crate::phone`]) and Nebo's Open Agent Link client read a
//! host's ACP updates through the same rules here, so a card, a mode or an
//! error reads the same on the phone and in Nebo.
//!
//! - [`Permission`] and [`mode_for`]: Nebo's permission mode for an
//!   employee, as the agent's own mode.
//! - [`words`] and [`words_in`]: a permission request as a question and its
//!   answers' labels.
//! - [`Tools`]: a session's tool cards, announced once the agent has said
//!   what each call is, their results once they finish ([`ToolEvent`]).
//! - [`plan`]: a plan as thinking text.
//! - [`plain`]: an error from the host as one sentence the owner reads.

use std::collections::HashMap;
use std::time::Instant;

use nebo_runtimes::acp::protocol::{PlanEntry, SessionMode, ToolCall as AcpToolCall, ToolStatus};
use serde_json::{Value, json};

use crate::model::{ErrorObject, PermissionOption, Words, code};

/// Where a runtime's own words for a permission request ride in the ACP
/// request's `_meta` (an adapted runtime, OpenClaw or Hermes, says its
/// question in its own words), so a client across Open Agent Link shows the
/// card the runtime wrote.
pub const WORDS_META: &str = "nebo/words";

/// How much the owner lets the employee do without asking: Nebo's
/// permission mode for it (`types::permissions::Mode`, as Nebo names it). A
/// runtime with modes of its own runs the turn in the one this maps to
/// ([`mode_for`]); one without ignores it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Permission {
    /// Every change asks.
    Ask,
    /// Acts inside its job (its folder); anything else asks.
    Automatic,
    /// Reads and plans; changes nothing.
    Plan,
    /// Nothing asks.
    FullAccess,
}

impl Permission {
    /// Nebo's name for the mode: `ask`, `automatic`, `plan`, `full_access`.
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "ask" => Some(Permission::Ask),
            "automatic" => Some(Permission::Automatic),
            "plan" => Some(Permission::Plan),
            "full_access" => Some(Permission::FullAccess),
            _ => None,
        }
    }
}

/// The agent's mode for a Nebo permission: by the ids Claude Code, Codex and
/// Gemini CLI use, else by the kind any agent may say its modes are.
///
/// | Nebo        | Claude Code         | Codex               | Gemini CLI | kind          |
/// |-------------|---------------------|---------------------|------------|---------------|
/// | Ask         | `default`           | `read-only`         | `default`  | `standard`    |
/// | Automatic   | `acceptEdits`       | `agent`             | `autoEdit` | `auto_review` |
/// | Plan        | `plan`              | (as Ask)            | `plan`     | `plan`        |
/// | Full access | `bypassPermissions` | `agent-full-access` | `yolo`     | `full_access` |
pub fn mode_for(permission: Permission, modes: &[SessionMode]) -> Option<&str> {
    let (ids, kinds): (&[&str], &[&str]) = match permission {
        Permission::Ask => (&["default", "read-only"], &["standard"]),
        Permission::Automatic => (&["acceptEdits", "agent", "autoEdit"], &["auto_review"]),
        Permission::Plan => (&["plan", "default", "read-only"], &["plan", "standard"]),
        Permission::FullAccess => (
            &["bypassPermissions", "agent-full-access", "yolo"],
            &["full_access"],
        ),
    };
    ids.iter()
        .find_map(|id| modes.iter().find(|m| m.id == *id))
        .or_else(|| {
            kinds
                .iter()
                .find_map(|kind| modes.iter().find(|m| m.kind.as_deref() == Some(*kind)))
        })
        .map(|m| m.id.as_str())
}

/// A plan as thinking text, one line per step.
pub fn plan(entries: &[PlanEntry]) -> String {
    let lines: Vec<String> = entries
        .iter()
        .map(|e| {
            let mark = match e.status.as_str() {
                "completed" => "[x]",
                "in_progress" => "[~]",
                _ => "[ ]",
            };
            format!("{mark} {}", e.content)
        })
        .collect();
    format!("Plan:\n{}", lines.join("\n"))
}

/// A permission request in the owner's words, from the call it asks about.
pub fn words(call: &AcpToolCall, options: &[PermissionOption]) -> Words {
    let label = call.label();
    let summary = match call.kind.as_deref() {
        Some("execute") => format!("run `{label}`"),
        Some("edit" | "delete" | "move") => format!("change {label}"),
        Some("fetch") => format!("fetch {label}"),
        _ => format!("use {label}"),
    };
    let question = match call.content.as_deref().filter(|c| *c != label) {
        Some(detail) => format!("{label}\n{detail}"),
        None => label.clone(),
    };
    let labels = options
        .iter()
        .map(|o| match o.kind.as_str() {
            "allow_once" => "Allow once".to_owned(),
            "allow_always" => "Always allow".to_owned(),
            "reject_once" => "Deny".to_owned(),
            "reject_always" => "Never allow".to_owned(),
            _ => o.name.clone(),
        })
        .collect();
    Words {
        question,
        summary,
        labels,
    }
}

/// The runtime's own words for a permission request, when its
/// `session/request_permission` params carry them ([`WORDS_META`]).
pub fn words_in(params: &Value) -> Option<Words> {
    serde_json::from_value::<Words>(params["_meta"][WORDS_META].clone())
        .ok()
        .filter(|w| !w.question.is_empty())
}

/// `params` with the runtime's own `words` in its `_meta`.
pub fn with_words(mut params: Value, words: &Words) -> Value {
    if !params["_meta"].is_object() {
        params["_meta"] = json!({});
    }
    params["_meta"][WORDS_META] = json!(words);
    params
}

/// An error from the host as the owner reads it, `name` being the agent's:
/// the host's plain words, or the agent's own with its name.
pub fn plain(name: &str, error: &ErrorObject) -> String {
    match error.code {
        code::NOT_FOUND => "That conversation was not found.".to_owned(),
        // Open Agent Link's plain reason is for its own clients; the owner
        // has always read this sentence.
        code::AGENT_UNAVAILABLE => format!("Could not connect to {name}. Try again."),
        code::AUTH_REQUIRED
        | code::TURN_FAILED
        | code::TURN_IN_PROGRESS
        | code::NOT_PERMITTED
        | code::UNKNOWN_AGENT
        | code::INTERNAL => error.message.clone(),
        _ => format!("{name}: {}", error.message),
    }
}

/// A tool card as a client shows it.
#[derive(Debug, Clone, PartialEq)]
pub enum ToolEvent {
    /// The agent said what the call is: it is running, finished, or asks.
    Started {
        id: String,
        name: String,
        input: Value,
    },
    /// The call finished.
    Finished {
        id: String,
        name: String,
        output: String,
        failed: bool,
        duration_ms: u64,
    },
}

/// A session's tool cards: each announced once the agent has said what the
/// call is (running, finished, or asking), its result once it finished.
#[derive(Default)]
pub struct Tools {
    calls: HashMap<String, Tool>,
    /// Tool ids in the order the agent opened them.
    order: Vec<String>,
    /// Cards to show, in order.
    out: Vec<ToolEvent>,
}

struct Tool {
    call: AcpToolCall,
    started: Instant,
    announced: bool,
    finished: bool,
}

impl Tools {
    /// Takes in a `tool_call` or `tool_call_update`; `duration_ms` is how long
    /// it took, where the runtime says.
    pub fn tool(&mut self, call: AcpToolCall, duration_ms: Option<u64>) {
        let id = call.id.clone();
        if !self.calls.contains_key(&id) {
            self.order.push(id.clone());
        }
        let tool = self.calls.entry(id.clone()).or_insert_with(|| Tool {
            call: AcpToolCall {
                id: id.clone(),
                ..AcpToolCall::default()
            },
            started: Instant::now(),
            announced: false,
            finished: false,
        });
        tool.call.merge(call);
        let status = tool.call.status;
        if matches!(status, Some(ToolStatus::InProgress)) || status.is_some_and(ToolStatus::is_done)
        {
            self.announce(&id);
        }
        let tool = self.calls.get_mut(&id).expect("tool");
        if let Some(status) = status.filter(|s| s.is_done())
            && !tool.finished
        {
            tool.finished = true;
            self.out.push(ToolEvent::Finished {
                id: id.clone(),
                name: tool.call.label(),
                output: tool.call.output(),
                failed: status == ToolStatus::Failed,
                duration_ms: duration_ms.unwrap_or(tool.started.elapsed().as_millis() as u64),
            });
        }
    }

    /// A permission request about `call`: its card is announced, and the
    /// request is in the owner's words from the call as the cards know it.
    pub fn ask(&mut self, call: AcpToolCall, options: &[PermissionOption]) -> Words {
        let id = call.id.clone();
        self.tool(call, None);
        self.announce(&id);
        words(&self.calls[&id].call, options)
    }

    /// Announces the call `id`, once.
    pub fn announce(&mut self, id: &str) {
        let Some(tool) = self.calls.get_mut(id) else {
            return;
        };
        if tool.announced {
            return;
        }
        tool.announced = true;
        let name = tool.call.label();
        self.out.push(ToolEvent::Started {
            id: id.to_owned(),
            name,
            input: tool.call.raw_input.clone().unwrap_or_else(|| json!({})),
        });
    }

    /// Announces every call not yet shown, before text that follows them.
    pub fn flush(&mut self) {
        for id in self.order.clone() {
            self.announce(&id);
        }
    }

    /// The cards made since last taken.
    pub fn take(&mut self) -> Vec<ToolEvent> {
        std::mem::take(&mut self.out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn offered(modes: &[(&str, &str)]) -> Vec<SessionMode> {
        modes
            .iter()
            .map(|(id, kind)| SessionMode {
                id: (*id).into(),
                kind: Some((*kind).into()),
            })
            .collect()
    }

    /// Each Nebo permission lands on the mode the adapters advertised on
    /// 2026-09-26 (claude-agent-acp 0.81.2, codex-acp 1.13.1), on Gemini
    /// CLI's (its ACP modes carry no kind), and on any other agent's by the
    /// kind it says the mode is.
    #[test]
    fn permissions_map_onto_the_agents_modes() {
        let claude = offered(&[
            ("default", "standard"),
            ("acceptEdits", "standard"),
            ("plan", "plan"),
            ("auto", "auto_review"),
            ("bypassPermissions", "full_access"),
        ]);
        let codex = offered(&[
            ("read-only", "standard"),
            ("agent", "auto_review"),
            ("agent-full-access", "full_access"),
        ]);
        // Gemini CLI's `--experimental-acp` modes, which say no kind.
        let gemini: Vec<SessionMode> = ["default", "autoEdit", "yolo", "plan"]
            .iter()
            .map(|id| SessionMode {
                id: (*id).into(),
                kind: None,
            })
            .collect();
        let other = offered(&[
            ("careful", "standard"),
            ("yolo", "full_access"),
            ("review", "auto_review"),
        ]);
        let cases = [
            (
                Permission::Ask,
                "default",
                "read-only",
                "default",
                Some("careful"),
            ),
            (
                Permission::Automatic,
                "acceptEdits",
                "agent",
                "autoEdit",
                Some("review"),
            ),
            (
                Permission::Plan,
                "plan",
                "read-only",
                "plan",
                Some("careful"),
            ),
            (
                Permission::FullAccess,
                "bypassPermissions",
                "agent-full-access",
                "yolo",
                Some("yolo"),
            ),
        ];
        for (permission, on_claude, on_codex, on_gemini, on_other) in cases {
            assert_eq!(
                mode_for(permission, &gemini),
                Some(on_gemini),
                "{permission:?}"
            );
            assert_eq!(
                mode_for(permission, &claude),
                Some(on_claude),
                "{permission:?}"
            );
            assert_eq!(
                mode_for(permission, &codex),
                Some(on_codex),
                "{permission:?}"
            );
            assert_eq!(mode_for(permission, &other), on_other, "{permission:?}");
        }
        // Claude Code without bypass offered: full access is not invented.
        let no_bypass = offered(&[("default", "standard"), ("acceptEdits", "standard")]);
        assert_eq!(mode_for(Permission::FullAccess, &no_bypass), None);
        assert_eq!(mode_for(Permission::Ask, &[]), None);
    }

    #[test]
    fn plans_read_as_steps() {
        let entries = vec![
            PlanEntry {
                content: "Read".into(),
                status: "completed".into(),
            },
            PlanEntry {
                content: "Fix".into(),
                status: "in_progress".into(),
            },
        ];
        assert_eq!(plan(&entries), "Plan:\n[x] Read\n[~] Fix");
    }

    /// A runtime's own words travel in the request's `_meta` and come back
    /// out whole; a request without them has none.
    #[test]
    fn a_runtimes_words_ride_in_meta() {
        let own = Words {
            question: "Hermes asks to run:\nrm -rf ./scratch".into(),
            summary: "run rm -rf ./scratch".into(),
            labels: vec![
                "Allow once".into(),
                "Allow for this conversation".into(),
                "Deny".into(),
            ],
        };
        let params = with_words(
            json!({ "sessionId": "s", "toolCall": { "toolCallId": "c" }, "options": [] }),
            &own,
        );
        assert_eq!(words_in(&params), Some(own));
        assert_eq!(
            params["toolCall"]["toolCallId"], "c",
            "the request is otherwise unchanged"
        );
        assert_eq!(words_in(&json!({ "sessionId": "s" })), None);
    }

    /// An asked-about call is announced before its card, and a finished one
    /// reports its output once.
    #[test]
    fn cards_are_announced_once_and_finish_once() {
        let mut tools = Tools::default();
        let call = AcpToolCall::parse(&json!({ "toolCallId": "c1", "title": "git status", "kind": "execute", "status": "pending" })).unwrap();
        let options = [PermissionOption {
            option_id: "allow".into(),
            name: "Allow".into(),
            kind: "allow_once".into(),
        }];
        let asked = tools.ask(call, &options);
        assert_eq!(
            (asked.question.as_str(), asked.labels.as_slice()),
            ("git status", &["Allow once".to_owned()][..])
        );
        let done = AcpToolCall::parse(&json!({ "toolCallId": "c1", "status": "completed",
            "content": [{ "type": "content", "content": { "type": "text", "text": "clean" } }] }))
        .unwrap();
        tools.tool(done.clone(), Some(5));
        tools.tool(done, Some(5));
        let cards = tools.take();
        assert_eq!(cards.len(), 2, "{cards:?}");
        assert!(
            matches!(&cards[0], ToolEvent::Started { id, name, .. } if id == "c1" && name == "git status")
        );
        assert!(
            matches!(&cards[1], ToolEvent::Finished { output, failed: false, duration_ms: 5, .. } if output == "clean")
        );
    }

    #[test]
    fn errors_read_plainly() {
        let unavailable = ErrorObject::new(
            code::AGENT_UNAVAILABLE,
            "Codex isn't signed in on this computer.",
        );
        assert_eq!(
            plain("Danny", &unavailable),
            "Could not connect to Danny. Try again."
        );
        assert_eq!(
            plain("Danny", &ErrorObject::new(code::NOT_FOUND, "x")),
            "That conversation was not found."
        );
        assert_eq!(
            plain("Danny", &ErrorObject::new(-32099, "the model is busy")),
            "Danny: the model is busy"
        );
    }
}
