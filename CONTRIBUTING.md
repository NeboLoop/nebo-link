# Contributing

Thank you for helping. This repository holds Nebo Link (the daemon and its runtime library), the Open Agent Link (OAL) specification in `spec/`, and its conformance suite in `crates/oal-conformance`.

## Sign your commits (DCO)

Every commit must carry a `Signed-off-by` line. It certifies the [Developer Certificate of Origin 1.1](https://developercertificate.org/): you wrote the change, or have the right to submit it under this repository's licenses.

```sh
git commit -s -m "Explain what the change does"
```

There is no contributor license agreement. Contributions are accepted under the licenses of the files they change: Apache-2.0 for code, CC-BY-4.0 for `spec/`.

## Changes to code

- Open an issue first for anything larger than a fix, so we can agree on the approach.
- Keep a pull request to one change. Include tests. `cargo test` must pass.
- Match the style of the code around your change.

## Changes to the protocol

The protocol changes through RFCs. See [spec/CONTRIBUTING.md](spec/CONTRIBUTING.md) and [GOVERNANCE.md](GOVERNANCE.md).

## Security issues

Do not open a public issue. See [SECURITY.md](SECURITY.md).

## Conduct

Everyone taking part follows the [Code of Conduct](CODE_OF_CONDUCT.md).
