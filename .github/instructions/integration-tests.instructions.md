---
description: "Use when creating, reviewing, or refactoring Go integration tests. Covers context-based file grouping, test-case matrix visibility, per-case descriptions, helper extraction, and how to explain integration-test changes in chat."
name: "Integration Test Guidelines"
applyTo: "**/*_integration_test.go"
---

# Integration Test Guidelines

## File Organization

- Group tests by context, not only by feature name or API surface.
- Keep tests that validate the same scenario family in the same file.
- Split a test into a dedicated file when it represents a distinct context that would otherwise get buried in a large mixed-purpose integration test file.
- Prefer file names that describe the context being tested.

## Test Shape

- Keep the main test focused on the behavior matrix for that context.
- Keep the test-case matrix in the main test function when that matrix is the clearest expression of what the test validates.
- Preserve a `name` field for subtest names.
- Add and preserve a separate `description` field for every test case.
- Make each `description` explain what the case proves, not just restate the method or input value.
- Extract only repeated mechanics into helpers, such as fixture setup, per-case execution, property building, and common assertions.
- Do not hide the behavioral matrix in a helper when the matrix is the core specification of the test.
- Prefer a shape where the main test communicates intent and coverage, and helpers contain only boilerplate.

## Coverage and Readability

- Preserve existing behavior and validation scope unless the task explicitly asks to broaden or reduce coverage.
- When refactoring, state clearly whether the change improves only readability or also changes coverage.
- If the readability gain is only incremental, say so directly.
- Keep end-to-end validation in place when the test verifies orchestration or API-selection behavior.

## Chat Output

- When describing or reviewing a test, explain first what it validates.
- When comparing old and new versions, state explicitly whether coverage changed.
- When proposing a new file, justify it in terms of context separation, not file length alone.
- Keep test-case descriptions visible in the chat output when they are part of understanding the test's intent.