# Releasing adk-aerospike to PyPI

Automated via [`.github/workflows/release.yml`](.github/workflows/release.yml): push a tag `vX.Y.Z` that matches `version` in `pyproject.toml` and `__version__` in `src/adk_aerospike/__init__.py`.

## One-time setup (maintainers)

### 1. PyPI trusted publisher

Use a PyPI account that can create projects for the Aerospike org (or register a **pending** publisher before the project exists).

1. Sign in at https://pypi.org/manage/account/publishing/
2. **Add a new pending publisher** (first release) or **Add a new publisher** (after the project exists).
3. Set:
   | Field | Value |
   | --- | --- |
   | PyPI project name | `adk-aerospike` |
   | Owner | `aerospike-community` |
   | Repository name | `adk-aerospike` |
   | Workflow name | `release.yml` |
   | Environment name | `pypi` |

The workflow filename is the basename only (`release.yml`), not `.github/workflows/release.yml`.

After the first successful publish, the pending publisher becomes a normal project publisher.

### 2. GitHub `pypi` environment

In https://github.com/aerospike-community/adk-aerospike/settings/environments:

1. **New environment** named `pypi` (must match PyPI’s environment name).
2. Recommended protection rules:
   - **Required reviewers** — one or two release approvers.
   - **Deployment branches** — only `main` (tags are evaluated against the commit they point at; restricting branches still limits who can trigger protected workflows from non-standard refs).
3. Under **Environment secrets** — none needed for trusted publishing (OIDC replaces API tokens).

### 3. Optional: TestPyPI dry run

1. Register at https://test.pypi.org/
2. Add another trusted publisher with the same owner/repo/workflow, or use a separate `testpypi` workflow and environment.
3. Publish manually once, then smoke-test: `pip install -i https://test.pypi.org/simple/ adk-aerospike`

## Cut a release

1. Bump version in **both** `pyproject.toml` and `src/adk_aerospike/__init__.py`.
2. Update `CHANGELOG.md`.
3. Merge to `main`.
4. Tag the **current `main` HEAD** (after your release PR is merged) and push:

   ```bash
   git checkout main && git pull
   git tag v0.0.2
   git push origin v0.0.2
   ```

   If a tag already exists on an older commit, delete it locally and on GitHub before re-tagging:

   ```bash
   git tag -d v0.0.2
   git push origin :refs/tags/v0.0.2
   git tag v0.0.2 && git push origin v0.0.2
   ```

5. In GitHub Actions, open the **Release** workflow run; approve the **pypi** environment deployment if reviewers are configured.
6. Confirm https://pypi.org/project/adk-aerospike/ shows the new version.
7. Smoke test from a clean venv:

   ```bash
   pip install "adk-aerospike==0.0.1"
   python -c "import adk_aerospike; print(adk_aerospike.__version__)"
   ```

8. Optional: create a GitHub Release with notes from `CHANGELOG.md`:

   ```bash
   gh release create v0.0.1 --notes-file CHANGELOG.md
   ```

## Local build check (no upload)

```bash
python -m pip install build
python -m build
python scripts/validate_pypi_metadata.py
pip install dist/adk_aerospike-*.whl
```

Benchmark harness deps are **not** in the PyPI package — install with `pip install -r benchmarks/requirements.txt`.

## Troubleshooting

| Symptom | Likely cause |
| --- | --- |
| Publish job fails immediately on OIDC | Trusted publisher owner/repo/workflow/environment mismatch |
| “File already exists” on upload | Version not bumped; PyPI versions are immutable |
| Tag/job fails at version check | Tag `v0.0.2` but `pyproject.toml` still `0.0.1` |
| `400 Can't have direct dependency` on upload | Tag points at a commit before the fix, or `[benchmark]` / git URL still in `pyproject.toml`; run `python scripts/validate_pypi_metadata.py` after `python -m build` |
| Validate step fails on release | Wheel still embeds VCS deps — remove optional extras with git URLs from `pyproject.toml` |
| Environment never appears | Workflow must reference `environment: name: pypi` on the publish job |
