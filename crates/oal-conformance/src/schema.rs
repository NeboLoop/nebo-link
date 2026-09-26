//! Checks frames against `spec/schemas/`.
//!
//! OAL's schemas reference ACP types instead of copying them. The suite
//! checks what OAL adds, so every ACP reference is accepted as it is:
//! whether an ACP message is valid ACP is ACP's own business.

use std::collections::HashMap;

use jsonschema::{Retrieve, Uri, Validator};
use serde_json::{Value, json};

use crate::spec::SCHEMAS;

/// Where OAL's schemas point for ACP types.
const ACP_SCHEMA: &str = "https://raw.githubusercontent.com/agentclientprotocol/agent-client-protocol/schema-v1.23.0/schema/v1/schema.json";
/// The `$id` base of OAL's schemas.
const BASE: &str = "https://openagent.link/schemas/0.1/";

pub struct Schemas {
    frame: Validator,
    error: Validator,
    /// `host/…` method → (params, result).
    methods: HashMap<String, (Validator, Option<Validator>)>,
}

impl Schemas {
    pub fn load() -> Self {
        let build = |schema: Value| {
            jsonschema::options()
                .with_retriever(Files)
                .build(&schema)
                .unwrap_or_else(|e| panic!("spec/schemas does not build: {e}"))
        };
        let mut methods = HashMap::new();
        for (file, text) in SCHEMAS {
            let doc: Value = serde_json::from_str(text).expect("schema JSON");
            let Some(method) = doc["title"].as_str().filter(|t| t.starts_with("host/")) else {
                continue;
            };
            let part = |name: &str| {
                doc["$defs"]
                    .get(name)
                    .map(|_| build(json!({ "$ref": format!("{BASE}{file}#/$defs/{name}") })))
            };
            let params = part("params").expect("every method schema has params");
            methods.insert(method.to_owned(), (params, part("result")));
        }
        Self {
            frame: build(json!({ "$ref": format!("{BASE}frame.schema.json") })),
            error: build(json!({ "$ref": format!("{BASE}error.schema.json") })),
            methods,
        }
    }

    /// Checks one frame. `answered` is the method of the request a
    /// host-channel response answers, when known.
    pub fn check(&self, frame: &Value, answered: Option<&str>) -> Result<(), String> {
        check(&self.frame, frame, "frame.schema.json")?;
        if frame.get("agent").is_some() {
            return Ok(());
        }
        if let Some(method) = frame["method"].as_str() {
            let (params, _) = self
                .methods
                .get(method)
                .ok_or_else(|| format!("{method} is not an OAL method"))?;
            return check(params, &frame["params"], &format!("{method} params"));
        }
        if let Some(error) = frame.get("error") {
            return check(&self.error, error, "error.schema.json");
        }
        match answered
            .and_then(|m| self.methods.get(m))
            .and_then(|(_, r)| r.as_ref())
        {
            Some(result) => check(
                result,
                &frame["result"],
                &format!("{} result", answered.unwrap_or("")),
            ),
            None => Ok(()),
        }
    }
}

fn check(validator: &Validator, value: &Value, what: &str) -> Result<(), String> {
    let errors: Vec<String> = validator
        .iter_errors(value)
        .map(|e| format!("{} at {}", e, e.instance_path()))
        .collect();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(format!("does not match {what}: {}", errors.join("; ")))
    }
}

/// Resolves `spec/schemas` files by name, and ACP's schema as a document
/// that accepts anything at every definition OAL names.
struct Files;

impl Retrieve for Files {
    fn retrieve(
        &self,
        uri: &Uri<String>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let uri = uri.as_str();
        if uri.starts_with(ACP_SCHEMA) {
            let defs: serde_json::Map<String, Value> = acp_names()
                .into_iter()
                .map(|n| (n, Value::Bool(true)))
                .collect();
            return Ok(json!({ "$defs": defs }));
        }
        let file = uri
            .strip_prefix(BASE)
            .ok_or_else(|| format!("no schema {uri}"))?;
        SCHEMAS
            .iter()
            .find(|(name, _)| *name == file)
            .map(|(_, text)| serde_json::from_str(text).expect("schema JSON"))
            .ok_or_else(|| format!("no schema {uri}").into())
    }
}

/// Every ACP definition the OAL schemas reference.
fn acp_names() -> Vec<String> {
    let marker = "schema.json#/$defs/";
    SCHEMAS
        .iter()
        .flat_map(|(_, text)| {
            text.match_indices(marker)
                .map(move |(i, _)| &text[i + marker.len()..])
        })
        .map(|rest| {
            rest.chars()
                .take_while(char::is_ascii_alphanumeric)
                .collect()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schemas_build_and_catch_a_bad_frame() {
        let schemas = Schemas::load();
        assert_eq!(schemas.methods.len(), 12, "one schema per host method");
        let turn = json!({ "jsonrpc": "2.0", "method": "host/turn", "params": {
            "agent": "app", "sessionId": "s", "turnId": "t1", "state": "running", "startedAt": "2026-09-26T17:04:05Z" } });
        schemas.check(&turn, None).unwrap();
        let mut bad = turn.clone();
        bad["params"]["state"] = json!("paused");
        assert!(
            schemas
                .check(&bad, None)
                .unwrap_err()
                .contains("host/turn params")
        );
        let frame = json!({ "agent": "app", "acp": { "jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {} } });
        schemas.check(&frame, None).unwrap();
        let both = json!({ "agent": "app", "acp": {}, "jsonrpc": "2.0" });
        assert!(
            schemas.check(&both, None).is_err(),
            "an agent frame has exactly two members"
        );
        let result = json!({ "jsonrpc": "2.0", "id": 1, "result": { "agents": [{ "id": "Bad Id", "label": "x", "runtime": "r", "online": true, "capabilities": {} }] } });
        assert!(schemas.check(&result, Some("host/agents")).is_err());
    }
}
