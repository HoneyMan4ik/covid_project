# COVID-19

##How To Run

1. Create and activate a virtual env, then install deps

```bash
python3 -m venv .venv
source .venv/bin/activate   # <- Linux/MacOS
# Windows: .venv\Scripts\activate
pip install --upgrade pip
pip install -r requirements.txt
```

2. Configure Snowflake

```bash
cp .env.template .env
```

3. Start the API (Terminal 1)

```bash
python -m uvicorn app.main:app --reload --port 8000
```

4. Start the dashboard (Terminal 2)

```bash
python app/dashboard.py
```

Open the dashboard: http://127.0.0.1:8051

API docs: http://127.0.0.1:8000/docs
