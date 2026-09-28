# Release Process

## Version

The version is derived automatically from the git tag by
[setuptools-scm](https://github.com/pypa/setuptools_scm). There is no version
string to edit manually. Tags must be plain PEP 440 versions without prefix
(e.g. `0.3.0`). Builds between tags get an automatically derived development
version such as `0.3.0.dev3+g7bf28c2`.

## Automated release (recommended)

Releases are created by the
[DataLad release action](https://github.com/datalad/release-action) via the
`.github/workflows/release.yml` workflow, which runs when a PR is merged into
`main`.

1. Add a changelog fragment under `changelog.d/` describing the change. PRs
   labelled with a category label can get one generated automatically by the
   `add-changelog-snippet` workflow.
2. Label the PR with a semver category (e.g. `semver-minor`, `semver-patch`)
   and the `release` label.
3. Merge the PR into `main`. The release workflow then:
   - collects the changelog fragments into `CHANGELOG.md`,
   - determines the version bump from the category labels,
   - creates and pushes the release tag (e.g. `0.3.0`),
   - creates a GitHub release.

The release can also be triggered manually from the Actions tab via the
**Run workflow** button on the *Release* workflow.

If the `pypi-token` input in `.github/workflows/release.yml` is supplied, the
action builds and uploads the distribution to PyPI as well.

## Manual build and upload

To build and upload a release by hand:

```bash
python -m build
twine upload dist/*
```

For testing on TestPyPI first:

```bash
twine upload --repository testpypi dist/*
```

To do a purely manual release, tag the current commit and push the tag:

```bash
git tag 0.3.0
git push origin 0.3.0
```
