# app_admin

Second FastAPI backend service for the monorepo.

Configuration is documented in `.env.example`, and runtime settings are loaded from `app/settings.py`.

Default API base path: `/api/admin/v1`

## Run

```bash
source .venv/bin/activate
uvicorn apps.app_admin.app.main:app --reload --port 8001
```

Endpoints:
- `GET /api/admin/v1/health`
- `GET /api/admin/v1/info`
- `POST /api/admin/v1/echo`

## Test

```bash
source .venv/bin/activate
pytest apps/app_admin/tests
```
