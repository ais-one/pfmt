# Contributing

## Getting Started

1. Fork and clone the repository.
2. Run the bootstrap script to set up your environment:

```bash
./scripts/bootstrap.sh
source .venv/bin/activate
```

This installs dependencies, registers git hooks, and activates the virtual environment.

## Development Workflow

### Branching

Refer to the followin document for branching instructions
- [https://github.com/es-labs/express-template/blob/main/docs/git.md#branch-tags-used]()
- [https://github.com/es-labs/express-template/blob/main/docs/git.md#branch--tag-summary--flow]()
- [https://github.com/es-labs/express-template/blob/main/docs/git.md#hotfix--backport-flow]()

### Commit messages

For standardized [Conventional Commits](https://www.conventionalcommits.org/) messages, use **commitizen** instead of `git commit -m "…"`:

```bash
# need to activate venv first
# source .venv/bin/activate
cz commit
```

Refer to [../pyproject.toml]() for commit types and scopes

### Code Style

This project uses [Ruff](https://docs.astral.sh/ruff/) for linting and formatting. Before committing, ensure your code passes:

```bash
ruff format .
ruff check .
```

The pre-commit hook runs these checks automatically on every commit.

### Testing

Run the full test suite before pushing:

```bash
pytest apps
```

The pre-push hook runs format checks, linting, and tests automatically.

### Versioning

Each service and `common` carries its own `__version__` string:

- `apps/*/app/version.py`
- `common/python/__init__.py`

To cut a release, use commitizen — it bumps all `__version__` files, generates a changelog entry, and creates a git tag in one step:

```bash
cz bump # bump version, tag, and update CHANGELOG.md
cz bump --dry-run  # preview without making changes
```

The version is surfaced at runtime via the `GET /api/admin/v1/info` endpoint and in the OpenAPI docs (`/docs`).

## Pull Requests

- Keep PRs focused — one concern per PR.
- Write a clear description of what changed and why.
- Ensure all hooks pass before opening a PR.
- Reference any related issues in the PR description.

## Project Structure

```
apps/          backend services
common/python/ shared modules used by all services
docs/          project documentation
scripts/       developer utility scripts
```

Refer to `README.md` for full setup and run instructions.
