# data_compare UI

A fully decoupled web interface for the data_compare framework.

## Architecture

```
Browser (HTML/CSS/JS)
        ↓
Flask API  (api/app.py)
        ↓
data_compare Framework  (data_compare_framework/)
```

## Quick Start

```bash
cd data_compare_ui
pip install flask pandas openpyxl PyYAML
python start.py
# Open http://localhost:5000
```

## Running Tests

```bash
cd data_compare_ui
python -m unittest tests/test_api.py -v
```

## File Structure

```
data_compare_ui/
├── api/
│   └── app.py          Flask REST API (all endpoints)
├── static/
│   └── index.html      Single-page application UI
├── tests/
│   └── test_api.py     15 integration + E2E tests
├── start.py            Startup script
├── requirements.txt
└── README.md
```

## API Endpoints

| Method | URL | Description |
|--------|-----|-------------|
| GET  | /api/health | Health check |
| POST | /api/detect-columns | Detect columns from a file path |
| POST | /api/run | Submit a comparison job |
| GET  | /api/job/<id> | Get job status and results |
| GET  | /api/job/<id>/logs | Get all log lines |
| GET  | /api/job/<id>/stream | SSE live log stream |
| GET  | /api/history | List all past runs |
| GET  | /api/capabilities | List registered capabilities |
| GET  | /api/download/<id>/<type> | Download excel/html/json report |
| DELETE | /api/job/<id> | Remove job from history |
