# Copilot Instructions for starburst-enterprise

## General Guidelines
- Follow the existing code style and conventions used in this repository.
- Prefer explicit, readable code over clever or overly compact solutions.
- All code must be compatible with Java 24 and the dependency versions specified in the root `pom.xml`.
- Use Airlift (airlift/airlift) as a main framework. Spring is not used at all.
- When adding new modules or dependencies, ensure they are included in the root `pom.xml` and follow the Maven structure.

## Documentation
- All public classes and methods should have Javadoc comments.
- Update or create relevant documentation when introducing new features or changes.

## Testing
- Add or update unit and integration tests for all new features and bug fixes.
- Use JUnit 5 for Java tests and follow the test patterns in the `testing/` modules.
- GitHub Actions are used for CI/CD, with self-hosted runners.

## Security
- Do not expose secrets, credentials, or proprietary information in code or documentation.
- Use environment variables for sensitive data in scripts.

## Pull Requests
- Verify that code follows the suggestions from [architecture guidelines](../architecture/project/structure.md).
- Ensure all checks pass before submitting a pull request.
- Provide a clear description of the change and reference related issues if applicable.
- Ensure that each change that modifies product API or configuration properties (it either comes with `@Config` annotation or directly from Trino dependency) is highlighted with comment `// [BREAKING]: [TIMESTAMP] [DESCRIPTION]` in the code.
- Use the `@Config` annotation for configuration properties and pair it with a `@ConfigDescription` annotation to provide a clear description.
- (applicable only if JIRA MCP is configured) When a breaking change is introduced, ensure that JIRA task linked in a pull request description has a task with name starting with `DOC-` added as a comment within it. This task should contain a description of the breaking change.

## Shell Scripts
- Use `set -euo pipefail` and validate all input parameters.
- Use `shellcheck` to lint all shell scripts.
- Prefer POSIX and GNU-compliant syntax if possible. If not, prefer GNU as GitHub Actions runners are Ubuntu Linux.

## Maven
- Keep dependencies up to date and avoid unnecessary version overrides.
- Use dependency management in the root `pom.xml` for all shared dependencies.

## Plugins
- When creating new plugins, follow the structure and naming conventions of existing plugins in the `plugin/` directory.
