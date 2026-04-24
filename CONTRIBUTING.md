# Contributing to kafka-sink-azure-kusto

Thanks for your interest in contributing! This document describes the
workflow for submitting changes and how CI treats pull requests.

## Getting started

1. Fork the repository (or create a branch directly in `Azure/kafka-sink-azure-kusto`
   if you have write access).
2. Create a feature branch off `master`.
3. Make your changes with accompanying unit tests.
4. Run the build locally:
   ```bash
   mvn clean verify -DskipITs
   ```
5. Open a pull request against `master`.

## Continuous integration

This repo uses a two-tier CI strategy so that PRs from forks can be built
safely without exposing Azure credentials to untrusted code.

### Tier 1 — Unit tests (automatic, every PR)

Workflow: [`.github/workflows/ci.yml`](.github/workflows/ci.yml)

- Runs on every `push` and every `pull_request`, including PRs from forks.
- Executes `mvn clean verify -DskipITs`: compile and unit tests (with
  integration tests skipped).
- Does **not** reference any secrets, so it always runs to completion
  regardless of who opened the PR.

You should expect this check to be green before a maintainer reviews your PR.

### Tier 2 — Integration tests (maintainer-gated)

Workflow: [`.github/workflows/integration.yml`](.github/workflows/integration.yml)

The integration tests in `src/test/java/**/it/` require real Azure
credentials (an Azure Data Explorer cluster, an AAD application, etc.).
Those credentials are stored as repository secrets and must never be
exposed to untrusted code. For that reason, integration tests run under
a separate, gated workflow.

**When they run automatically**

- On every push to `master` (already trusted).

**When they run for a pull request**

Only after a maintainer applies the **`safe-to-test`** label to the PR.
GitHub's built-in "Approve and run" button for first-time contributors
does **not** expose secrets to fork PRs — labeling via `pull_request_target`
is how we grant that access in a controlled way.

**Hardening applied to the gate**

1. The label must be applied by a user with `write`, `maintain`, or
   `admin` permission on the repository. The workflow verifies this
   using the GitHub REST API before anything else runs.
2. The `safe-to-test` label is **automatically removed** every time new
   commits are pushed to the PR. A maintainer must review the new
   commits and re-apply the label for each revision.
3. The workflow checks out the PR head by its exact commit SHA and
   disables credential persistence on the checkout step.

### Maintainer checklist (triggering integration tests on a fork PR)

1. Review the diff for anything suspicious — build scripts, workflow
   files, shell invocations in tests, new dependencies, etc. Treat the
   PR like untrusted code.
2. Apply the `safe-to-test` label to the PR.
3. The `Integration Tests` workflow will start. Watch the run.
4. If the contributor pushes more commits, the label will be stripped
   automatically. Re-review and re-apply when you're satisfied.

To create the label the first time (one-off setup):

```bash
gh label create safe-to-test \
  --description "Allow integration tests to run against this PR" \
  --color 0E8A16
```

## Reporting security issues

Please do **not** file security issues as public GitHub issues. See
[`SECURITY.md`](SECURITY.md) for the responsible disclosure process.
