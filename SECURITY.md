<!--
Copyright 2026 The PyAthena authors

Licensed under the MIT License.
See LICENSE or https://opensource.org/licenses/MIT.

SPDX-License-Identifier: MIT
-->

# Security Policy

## Supported versions

Security fixes are released for the latest version of PyAthena only. There are no backports to
earlier release series, so please upgrade to the latest release before reporting an issue.

## Reporting a vulnerability

Please report security vulnerabilities privately, not through a public issue or pull request,
since a public report discloses the problem before a fix is available.

Use GitHub's private vulnerability reporting: go to the
[Security tab](https://github.com/pyathena-dev/PyAthena/security) of this repository and click
**Report a vulnerability**. This opens a private channel visible only to the maintainers.

A report is most useful when it includes:

- the PyAthena version you tested against
- the affected code path, and the API you called to reach it
- steps to reproduce, ideally without requiring a live AWS account
- what an attacker can do with it

Proof-of-concept code is welcome but not required. Please do not test against AWS accounts or
data that you do not own.

## What to expect

This is a community-maintained project, so response times depend on maintainer availability.
The usual process is:

1. The report is acknowledged and the affected code path is confirmed.
2. A fix is prepared privately, along with regression tests.
3. The fix is released to PyPI, and a
   [GitHub Security Advisory](https://github.com/pyathena-dev/PyAthena/security/advisories) is
   published the same day, so that users are notified only once an upgrade is available.
4. Reporters are credited in the advisory unless they ask not to be.

If a CVE ID is warranted, it is requested through GitHub or coordinated with the CNA that
assigned it.
