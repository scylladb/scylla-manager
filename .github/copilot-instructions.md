# Copilot PR Review Guidelines

## Scope

- For pull request review requests, produce findings only for files and lines changed in the current PR diff.
- Do not create blocking findings for unchanged files.
- If you detect similar issues outside the diff, include them in an `Optional Follow-ups (Out of Scope)` section.
- Do not treat out-of-scope follow-ups as required changes for the current PR.

## Review Focus

- Prioritize correctness, behavior changes, regressions, and missing or weak tests.
- Keep summaries short and place findings first.
- Use file and line references for each finding.

## Integration Tests

- Follow `.github/instructions/integration-tests.instructions.md` when reviewing or refactoring integration tests.
- When evaluating integration test refactors, state clearly whether coverage changed or only readability changed.
