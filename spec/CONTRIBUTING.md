# Changing Open Agent Link

OAL changes through public RFCs. This keeps every change written down, argued in the open, and tested before anyone depends on it.

## What needs an RFC

- Any new or changed message, field, error code, close code or behaviour a host or client must follow.
- Any change to the transport, pairing, authentication or encryption.
- Deprecating or removing anything.

Wording fixes, clarifications that change no behaviour, and new examples need only a pull request.

## The process

1. **Draft.** Copy `rfcs/0000-template.md` to `rfcs/0000-short-name.md` and fill it in. Open a pull request titled `RFC: <name>`.
2. **Number.** A maintainer assigns the next number and renames the file.
3. **Discuss.** Comments on the pull request. The author updates the RFC as it changes.
4. **Prove it.** Before acceptance, an RFC that changes behaviour includes:
   - the text change to `oal-<version>.md`;
   - the schema change in `schemas/`;
   - at least one example in `examples/` showing it;
   - the conformance suite change in `crates/oal-conformance` that tests it.
5. **Decide.** The maintainers accept or decline, and the RFC records the decision and why.
6. **Release.** Accepted RFCs ship in the next version of the specification.

## Versions

- A version is `MAJOR.MINOR` (section 13 of the spec). Before 1.0, a minor version may break compatibility; from 1.0, only a major version may.
- Each version is its own file (`oal-0.1.md`, `oal-0.2.md`, …) and its schemas carry the version in their `$id` (`https://openagent.link/schemas/<version>/`). A published version is never edited except for errata that change no behaviour.
- Hosts and clients advertise the range of versions they support; section 13 says how one is chosen.

## Deprecation

A feature is deprecated in one version and may be removed no earlier than the **next minor version** after it. So the window is at least one minor version. The version that deprecates it says so in its changes, and names what to use instead. Hosts and clients should warn in their logs when the other side uses a deprecated feature.

## Compatibility with ACP

OAL carries ACP unchanged. A change to how ACP messages behave belongs in ACP itself, proposed to the ACP project. OAL RFCs cover only what OAL adds.
