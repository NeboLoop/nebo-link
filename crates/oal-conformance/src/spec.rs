//! The spec files the suite runs, embedded so the binary stands alone.

/// A recorded example from `spec/examples/`.
pub struct Example {
    pub name: &'static str,
    pub json: &'static str,
}

macro_rules! example {
    ($name:literal) => {
        Example {
            name: $name,
            json: include_str!(concat!("../../../spec/examples/", $name, ".json")),
        }
    };
}

/// Every example, in the order a run uses them: `pair` first, because the
/// others use the device it pairs.
pub const EXAMPLES: &[Example] = &[
    example!("pair"),
    example!("agents"),
    example!("prompt-permission"),
    example!("reconnect"),
    example!("cancel"),
    example!("mode"),
    example!("first-answer-wins"),
    example!("version-mismatch"),
];

pub fn example(name: &str) -> Option<&'static Example> {
    EXAMPLES.iter().find(|e| e.name == name)
}

macro_rules! schema {
    ($file:literal) => {
        (
            $file,
            include_str!(concat!("../../../spec/schemas/", $file)),
        )
    };
}

/// Every file in `spec/schemas/`, by file name.
pub const SCHEMAS: &[(&str, &str)] = &[
    schema!("defs.schema.json"),
    schema!("error.schema.json"),
    schema!("frame.schema.json"),
    schema!("host-agent-update.schema.json"),
    schema!("host-agents.schema.json"),
    schema!("host-answer.schema.json"),
    schema!("host-devices.schema.json"),
    schema!("host-hello.schema.json"),
    schema!("host-info.schema.json"),
    schema!("host-pair.schema.json"),
    schema!("host-pending-update.schema.json"),
    schema!("host-pending.schema.json"),
    schema!("host-ping.schema.json"),
    schema!("host-turn.schema.json"),
    schema!("host-unpair.schema.json"),
];
