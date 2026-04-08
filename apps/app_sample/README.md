# app_sample

Sample FastAPI backend service for the monorepo.

Configuration is documented in `.env.example`, and runtime settings are loaded from `app/settings.py`.

Default API base path: `/api/sample/v1`

## Run

```bash
source .venv/bin/activate
uvicorn apps.app_sample.app.main:app --reload
```

Endpoints:
- `GET /api/sample/v1/health`
- `POST /api/sample/v1/echo`

## Test

```bash
source .venv/bin/activate
pytest apps/app_sample/tests
```
