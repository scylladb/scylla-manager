---
name: "Refactor Integration Test"
description: "Use when refactoring a Go integration test while preserving coverage. Helps reorganize tests by context, keep the behavior matrix visible, and extract only repetitive mechanics."
argument-hint: "Name the integration test or file to refactor and any constraints about what must remain visible in the main test."
agent: "agent"
---

Refactor the target Go integration test.

Follow the guidance from [Integration Test Guidelines](../instructions/integration-tests.instructions.md).

Requirements:

- Preserve behavior and coverage unless explicitly asked to change them.
- Group tests by context.
- If the test belongs to a distinct context, move it to a dedicated `*_integration_test.go` file.
- Keep the test-case matrix in the main test function when that matrix is the clearest statement of what the test validates.
- Preserve `name` for subtest output.
- Add or preserve a separate `description` field for every test case.
- Extract only repeated mechanics into helpers.
- Keep helper names scenario-specific and avoid generic utility sprawl.
- Prefer the smallest refactor that materially improves readability.

Before editing:

- Inspect existing integration test patterns in the same package.
- Identify whether the current file mixes multiple contexts.
- Decide whether the refactor should stay in place or move the test to another file.

After editing:

- Run the narrowest relevant integration test target.
- Report whether coverage changed or only readability changed.
- Explain why the chosen file organization better reflects test context.