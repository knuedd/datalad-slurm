# Release Process

## Version Bump

Update version in two files:

1. `pyproject.toml` line 10:
   ```toml
   version = "x.y.z"
   ```

2. `src/datalad_slurm/__init__.py` line 49:
   ```python
   __version__ = "x.y.z"
   ```

## Build and Upload

```bash
python -m build
twine upload dist/*
```

For testing on TestPyPI first:
```bash
twine upload --repository testpypi dist/*
```