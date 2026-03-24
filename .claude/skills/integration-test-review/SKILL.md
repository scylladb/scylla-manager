---
name: integration-test-review
description: Review changes to the new or already existing integration tests from `*_integration_test.go` files. Do this before making the changes yourself or when asked to review them.
---

## Review integration tests changes

Changes made to integration tests should be reviewed while working on them or when asked to review them.
When working on integration tests, make sure to follow the review comments.

## Scope

Analyze all connected integration tests, but focus and generate comments only for the ones changed in the current scope.
The scope can be the current PR scope or a specific set of last commits.

## Integration test requirements

Integration tests should meet the following criteria:
- Test name should start with `Test` followed by service name (if applicable) and end with `Integration` (e.g., `TestBackupSmokeIntegration`)
- They should start with a comment explaining which features are tested
- They should follow the table-driven approach whenever multiple scenarios are tested. Table entry should contain at least the test name and description
- Each tested feature should be covered by a separate test of subtest, unless it's too small to justify such separation
- Tested feature should be tested in a scope of a single test or subtest, unless it's not orthogonal to other features. In such cases, both single feature test and combined features tests can coexist
- Test code should be clearly separated to set up and validation stages with the help of helper functions
- Test code should cover the changes made in the current scope
- Test code should follow established best practices
- Test code should be comprehensive, and it should be clear why it succeeds or fails

## Review summary

Review should end with a summary containing the following points:
- whether the integration test requirements are met
- whether the added integration tests cover the changes made by in current scope
- whether there is a better way of implementing or organizing changed integration tests. If so, propose it
- whether generated review comments must be addressed before merging, or they are optional follow-ups or nitpicks
- when changing existing integration tests, whether the changes didn't decrease test coverage