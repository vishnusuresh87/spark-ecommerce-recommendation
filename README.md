# spark-ecommerce-recommendation
Build a Scalable E-Commerce Recommendation Pipeline using databricks, cloud pipelines, git.

## Testing

This project uses `pytest` with two test labels (markers):
- `unit` for fast local checks (no Databricks table dependencies)
- `integration` for Spark + Unity Catalog data-quality checks

### Quick check (recommended before every commit)

```powershell
pytest -m unit
```

### Full check (after running the pipeline in Databricks)

```powershell
pytest
```

### Run only integration checks

```powershell
pytest -m integration
```

### Databricks note (fixes `__pycache__` Errno 95)

If you run tests from `/Workspace/...`, use no-bytecode mode:

```python
import os, sys, subprocess

repo = "/Workspace/Users/vishnusuresh87@gmail.com/spark-ecommerce-recommendation"
env = os.environ.copy()
env["PYTHONDONTWRITEBYTECODE"] = "1"

subprocess.run(
	[sys.executable, "-B", "-m", "pytest", "-m", "integration", "-vv", "-p", "no:cacheprovider"],
	cwd=repo,
	env=env,
)
```

Or run the helper script:

```python
import sys, subprocess

repo = "/Workspace/Users/vishnusuresh87@gmail.com/spark-ecommerce-recommendation"
subprocess.run([sys.executable, "scripts/run_integration_tests_databricks.py"], cwd=repo)
```
