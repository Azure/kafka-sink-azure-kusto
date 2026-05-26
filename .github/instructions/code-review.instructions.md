---
applyTo: "**/*.java,**/pom.xml,**/*.kql,**/*.properties,**/*.yml,**/*.yaml"
description: >
  IDE pointer for VS Code Copilot. The canonical, full review prompt
  lives at .github/copilot-instructions.md and is also consumed by
  GitHub Copilot PR review.
---

# Code review (IDE scope)

The full **CodeRabbit-style** code-review reviewer prompt — covering the
three-section output structure (Architectural Summary, Automated
Findings feed with severity-tagged 🔴/⚠️/ℹ️/❓/✅ entries, mandatory
Code Generation Guardrails with diffs), 11 domain-specific check
suites (security & injection, performance & batching, concurrency &
lifecycle, resiliency & DLQ, Kafka Connect contract, Kusto / ADX
specifics, observability, supply chain, testing, docs, design
principles), worked visual examples, project-specific anchors, and the
final verification checklist —
lives at:

> [`.github/copilot-instructions.md`](../copilot-instructions.md)

That file is the single source of truth. It is consumed by:

1. **GitHub Copilot PR review** (auto, when Copilot code review is
   enabled on the repository).
2. **VS Code Copilot** (this file scopes the same guidance to the file
   types most likely to need it, via the `applyTo` frontmatter).

When editing or improving the review guidance, edit
`.github/copilot-instructions.md` — not this file. Keep this file as
a thin pointer so the two surfaces stay in sync.
