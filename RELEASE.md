# Release Process

## Version

The version is derived automatically from the git tag by
[setuptools-scm](https://github.com/pypa/setuptools_scm). There is no
version string to edit manually.

To make a release, create and push an annotated (or lightweight) tag:

```bash
git tag 0.3.0
git push origin 0.3.0
```

Tags must be plain PEP 440 versions (e.g. `0.3.0`). Builds between tags get
an automatically derived development version such as `0.3.0.dev3+g7bf28c2`.

## Build and Upload

```bash
python -m build
twine upload dist/*
```

For testing on TestPyPI first:
```bash
twine upload --repository testpypi dist/*
```
