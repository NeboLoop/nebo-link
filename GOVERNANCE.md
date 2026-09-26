# Governance

## Maintainers

The maintainers are the NeboAI team today. They review and merge pull requests, decide on RFCs, and cut releases of Nebo Link and of the Open Agent Link specification.

Outside contributors can become maintainers. After sustained contributions of good quality (code, reviews, RFCs, or help for other users), a maintainer may nominate a contributor. The existing maintainers decide by consensus. The maintainers are listed in this file as the list grows.

## How the protocol changes

Open Agent Link changes through RFCs, never through code alone.

1. Copy `spec/rfcs/0000-template.md` to `spec/rfcs/0000-short-name.md` and open a pull request. The maintainers give it the next number.
2. Discussion happens on the pull request. An RFC that changes messages includes the schema and example changes, and conformance tests where they apply.
3. The maintainers accept or decline it and record why in the RFC.
4. An accepted RFC is folded into the next version of the specification.

The details, including versioning and the deprecation window, are in [spec/CONTRIBUTING.md](spec/CONTRIBUTING.md).

## Decisions about code

Code changes are decided in pull requests. A maintainer other than the author approves before merge. Disagreements that a pull request can't settle go to an issue, and the maintainers decide.

## Licenses

Code is Apache-2.0 (`LICENSE`). The specification in `spec/` is CC-BY-4.0 (`spec/LICENSE`). Contributions are signed off under the DCO (`CONTRIBUTING.md`).
