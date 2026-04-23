# Racing Assistant

Starter project for an automated horse racing data pipeline, feature engine, prediction engine, paper betting engine, and AI assistant.

## Quick start

1. Open this folder in VS Code.
2. Create a virtual environment:
   - macOS/Linux: `python3 -m venv venv && source venv/bin/activate`
   - Windows: `python -m venv venv && venv\\Scripts\\activate`
3. Install packages:
   - `pip install -r requirements.txt`
4. Copy `.env.example` to `.env` and add your API key if needed.
5. Run the pipeline:
   - `python3 -m app.main`

> On macOS, if `python` is not available, use `python3` or add this alias to `~/.zshrc`:
>
> ```bash
> alias python=python3
> ```

## What this version includes

- SQLite database setup
- SQLAlchemy models
- Sample data updaters for meetings, runners, and odds
- Feature computation
- Simple baseline prediction engine
- Paper betting engine
- Daily summary report

## Important

This version is a working scaffold. It uses sample data so you can test the structure before replacing the pipeline scripts with real data ingestion.
