# Security policy

Nebo Link lets people reach agents on their computers from other devices, so we treat security reports as our highest priority.

## Reporting a vulnerability

Email **security@neboai.com**. Please do not open a public issue, pull request or discussion about it.

Include what you found, how to reproduce it, the versions affected, and the impact you expect. We will acknowledge your report within three business days and keep you updated as we fix it.

## Disclosure

We ask for private disclosure and a window of **90 days** from your report to fix the problem and release the fix before you publish details. If a fix ships sooner, we will agree a publication date with you. If a problem is being exploited, we may publish sooner, together with you.

We credit reporters in the release notes unless you ask us not to.

## Scope

- The `nebo-link` daemon and runtime library in this repository.
- The Open Agent Link specification in `spec/` (design flaws in pairing, authentication, permission handling or encryption).
- The conformance suite, where a flaw would let a non-conforming host pass.

Section 16 of the specification (`spec/oal-0.1.md`) states what OAL 0.1 does and does not protect, including that a relay can read traffic until end-to-end encryption ships.
