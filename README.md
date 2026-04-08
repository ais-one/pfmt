# pfa

Python backend monorepo for FastAPI development using `pyenv`, `pip`, and `venv`.

## Read Me First

- Contributors: read `.github/copilot-instructions.md` for workspace-specific project guidance.
- Developers: use `.python-version`, a local `.venv`, and `pip` requirements files for dependency management.

## Structure

- `apps/` backend services
- `common/python/` shared backend modules
- `docs/` project documentation
- `scripts/` developer utility scripts

## Services

- `apps/app_sample` sample public-facing FastAPI service mounted under `/api/sample/v1`
- `apps/app_admin` second FastAPI service mounted under `/api/admin/v1`
- `common/python` shared configuration helpers, logging, and request/response models used by both services

## Service Configuration

- `apps/app_sample/.env.example` documents the environment variables for the sample service.
- `apps/app_admin/.env.example` documents the environment variables for the admin service.
- Each service has its own settings module under `app/settings.py`.
- Shared environment parsing helpers, request/response models, and test helpers live under `common/python`.

## Setup

```bash
./scripts/bootstrap.sh
source .venv/bin/activate
```

If you prefer to bootstrap manually:

```bash
pyenv install -s $(cat .python-version)
pyenv local $(cat .python-version)
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements-dev.txt -r apps/app_sample/requirements.txt

for file in apps/*/requirements.txt; do
	pip install -r "$file"
done
```

## Run The Sample Service

```bash
source .venv/bin/activate
uvicorn apps.app_sample.app.main:app --reload
```

Sample endpoints:
- `GET /api/sample/v1/health`
- `POST /api/sample/v1/echo`

## Run The Second Service

```bash
source .venv/bin/activate
uvicorn apps.app_admin.app.main:app --reload --port 8001
```

Admin endpoints:
- `GET /api/admin/v1/health`
- `GET /api/admin/v1/info`
- `POST /api/admin/v1/echo`

## Run Tests

```bash
source .venv/bin/activate
pytest apps
```
