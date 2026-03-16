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
