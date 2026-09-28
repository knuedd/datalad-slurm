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

When the *Release* workflow finishes successfully, the
`.github/workflows/publish.yml` workflow runs, builds the distribution, and
uploads it to PyPI using
[trusted publishing](https://docs.pypi.org/trusted-publishers/) (OIDC), so no
PyPI token is stored anywhere.

> **Note:** `publish.yml` triggers on `workflow_run` completion of the
> *Release* workflow, **not** on `release: published`. The DataLad release
> action creates the GitHub release with `GITHUB_TOKEN`, and GitHub does not
> start new workflow runs from `GITHUB_TOKEN`-triggered events, so a
> `release: published` trigger would silently never fire.

### Releasing to PyPI next time

1. Let the *Release* workflow create the new tag and GitHub release (see
   above). The bump is automatic.
2. That successful run automatically starts *Publish to PyPI*; wait for it to
   finish and check <https://pypi.org/project/datalad-slurm/#history>.
3. If it did **not** run or failed, trigger it by hand: Actions →
   **Publish to PyPI** → **Run workflow** → branch `main`. This builds whatever
   version the tip of `main` resolves to, so only do it once the release tag
   exists.

The one-time trusted-publisher registration on PyPI is described below; it does
not need repeating for every release.

### Trusted publishing setup (one-time)

The trusted publisher is configured on PyPI, not in the repository. At
<https://pypi.org/manage/project/datalad-slurm/settings/publishing/> add a
GitHub publisher with:

- Owner: `knuedd`
- Repository: `datalad-slurm`
- Workflow name: `publish.yml`
- Environment: `pypi`

The workflow name and environment must match `publish.yml` exactly. Note that
`publish.yml` must be present on the repository's default branch (`develop`),
because PyPI resolves the trusted publisher against the default branch and
that is where GitHub looks up `workflow_run` workflows.

Because trusted publishing relies on an OIDC identity token issued by GitHub
Actions, it only works from a supported CI environment. It cannot be used from
a local shell; for manual uploads use a PyPI API token instead (see below).

## Manual build and upload

To build and upload a release by hand, check out the commit that carries the
release tag first. Outside of a tag, setuptools-scm produces a development
version with a local segment (e.g. `0.2.6.dev14+gf80bc2696`), which PyPI
rejects with `400 ... local versions ... not allowed`.

```bash
git checkout 0.3.0   # the release tag
rm -rf dist build src/*.egg-info
python -m build
twine check dist/*   # confirm the version and metadata
```

Then upload with a PyPI API token (create one at
<https://pypi.org/manage/account/token/>):

```bash
TWINE_USERNAME=__token__ TWINE_PASSWORD=pypi-... twine upload dist/*
```

For testing on TestPyPI first:

```bash
TWINE_USERNAME=__token__ TWINE_PASSWORD=pypi-... \
  twine upload --repository testpypi dist/*
```

To do a purely manual release, tag the current commit and push the tag:

```bash
git tag 0.3.0
git push origin 0.3.0
```
