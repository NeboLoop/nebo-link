//! Every recorded example passes against the fake host, driven by the fake
//! client; so the examples, the fake host and the matcher agree with each
//! other and with the schemas.

use std::process::Stdio;

use oal_conformance::fake_host::{self, Config};
use oal_conformance::spec::{self, Example};
use oal_conformance::transcript::{self, Target};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

async fn target() -> Target {
    let addr = fake_host::serve("127.0.0.1:0".parse().unwrap(), Config::default())
        .await
        .unwrap();
    Target {
        url: format!("ws://{addr}/oal"),
        pair_url: None,
        headers: Vec::new(),
        known: vec![
            ("code".into(), json!("K7QM-3XRD")),
            ("agent".into(), json!(fake_host::AGENT)),
        ],
    }
}

/// Runs `name` after the examples that capture what it needs.
async fn passes(name: &str) {
    let mut examples: Vec<&'static Example> = vec![
        spec::example("pair").unwrap(),
        spec::example("agents").unwrap(),
    ];
    if !examples.iter().any(|e| e.name == name) {
        examples.push(spec::example(name).unwrap());
    }
    for outcome in transcript::run(&target().await, &examples).await {
        if let Err(e) = outcome.result {
            panic!("{}: {e}", outcome.example);
        }
    }
}

#[tokio::test]
async fn pair() {
    passes("pair").await;
}

#[tokio::test]
async fn agents() {
    passes("agents").await;
}

#[tokio::test]
async fn prompt_permission() {
    passes("prompt-permission").await;
}

#[tokio::test]
async fn reconnect() {
    passes("reconnect").await;
}

#[tokio::test]
async fn cancel() {
    passes("cancel").await;
}

#[tokio::test]
async fn mode() {
    passes("mode").await;
}

#[tokio::test]
async fn first_answer_wins() {
    passes("first-answer-wins").await;
}

#[tokio::test]
async fn version_mismatch() {
    passes("version-mismatch").await;
}

#[test]
fn every_example_is_run() {
    let files: Vec<String> =
        std::fs::read_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/../../spec/examples"))
            .unwrap()
            .filter_map(|e| {
                e.ok()?
                    .file_name()
                    .into_string()
                    .ok()?
                    .strip_suffix(".json")
                    .map(str::to_owned)
            })
            .collect();
    assert_eq!(
        files.len(),
        spec::EXAMPLES.len(),
        "spec/examples and spec::EXAMPLES differ: {files:?}"
    );
    for file in files {
        assert!(
            spec::example(&file).is_some(),
            "{file}.json is not in spec::EXAMPLES"
        );
    }
}

/// A host that breaks an example fails it, with the step it broke on.
#[tokio::test]
async fn a_wrong_answer_fails_the_example() {
    let broken: &'static str = Box::leak(
        spec::example("agents")
            .unwrap()
            .json
            .replace(r#""online": true"#, r#""online": false"#)
            .into_boxed_str(),
    );
    let examples: Vec<&'static Example> = vec![
        spec::example("pair").unwrap(),
        Box::leak(Box::new(Example {
            name: "agents-broken",
            json: broken,
        })),
    ];
    let outcomes = transcript::run(&target().await, &examples).await;
    assert!(outcomes[0].result.is_ok());
    let error = outcomes[1].result.as_ref().unwrap_err();
    assert!(
        error.contains("step 5") && error.contains("timed out"),
        "{error}"
    );
}

/// `oal-conformance agent` speaks ACP on stdio, for a host under test.
#[tokio::test]
async fn the_stdio_agent_answers_initialize() {
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_oal-conformance"))
        .arg("agent")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let request = json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize", "params": { "protocolVersion": 1 } });
    stdin
        .write_all(format!("{request}\n").as_bytes())
        .await
        .unwrap();
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    let answer: Value = serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
    assert_eq!(answer["id"], 1);
    assert_eq!(answer["result"]["agentCapabilities"]["loadSession"], true);
}
