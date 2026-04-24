from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker, declarative_base
from app.config import DATABASE_URL

engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()


def init_db() -> None:
    from app import models  # noqa: F401
    Base.metadata.create_all(bind=engine)
    _ensure_races_columns()
    _ensure_paper_bets_columns()
    _ensure_paper_bet_archives_columns()
    _ensure_paper_bank_resets_columns()


def _ensure_races_columns() -> None:
    with engine.begin() as connection:
        columns = {
            row[1]
            for row in connection.execute(text("PRAGMA table_info(races)"))
        }
        if "betfair_market_id" not in columns:
            connection.execute(
                text("ALTER TABLE races ADD COLUMN betfair_market_id VARCHAR")
            )


def _ensure_paper_bets_columns() -> None:
    with engine.begin() as connection:
        columns = {
            row[1]
            for row in connection.execute(text("PRAGMA table_info(paper_bets)"))
        }
        required_columns = {
            "commission_rate": "ALTER TABLE paper_bets ADD COLUMN commission_rate FLOAT DEFAULT 0.08",
            "paper_bank_reset_id": "ALTER TABLE paper_bets ADD COLUMN paper_bank_reset_id INTEGER",
            "form_score": "ALTER TABLE paper_bets ADD COLUMN form_score FLOAT",
            "combined_score": "ALTER TABLE paper_bets ADD COLUMN combined_score FLOAT",
            "qualification_reason": "ALTER TABLE paper_bets ADD COLUMN qualification_reason VARCHAR",
            "last_start_finish": "ALTER TABLE paper_bets ADD COLUMN last_start_finish FLOAT",
            "avg_last3_finish": "ALTER TABLE paper_bets ADD COLUMN avg_last3_finish FLOAT",
            "avg_last3_margin": "ALTER TABLE paper_bets ADD COLUMN avg_last3_margin FLOAT",
            "closing_odds": "ALTER TABLE paper_bets ADD COLUMN closing_odds FLOAT",
            "final_observed_odds": "ALTER TABLE paper_bets ADD COLUMN final_observed_odds FLOAT",
            "closing_line_difference": "ALTER TABLE paper_bets ADD COLUMN closing_line_difference FLOAT",
            "closing_line_pct": "ALTER TABLE paper_bets ADD COLUMN closing_line_pct FLOAT",
            "clv_percent": "ALTER TABLE paper_bets ADD COLUMN clv_percent FLOAT",
            "beat_closing_line": "ALTER TABLE paper_bets ADD COLUMN beat_closing_line BOOLEAN",
            "placed_at": "ALTER TABLE paper_bets ADD COLUMN placed_at DATETIME",
            "settled_at": "ALTER TABLE paper_bets ADD COLUMN settled_at DATETIME",
        }

        for column_name, ddl in required_columns.items():
            if column_name not in columns:
                connection.execute(text(ddl))


def _ensure_paper_bank_resets_columns() -> None:
    with engine.begin() as connection:
        columns = {
            row[1]
            for row in connection.execute(text("PRAGMA table_info(paper_bank_resets)"))
        }
        required_columns = {
            "decision_version": "ALTER TABLE paper_bank_resets ADD COLUMN decision_version VARCHAR",
        }

        for column_name, ddl in required_columns.items():
            if column_name not in columns:
                connection.execute(text(ddl))


def _ensure_paper_bet_archives_columns() -> None:
    with engine.begin() as connection:
        columns = {
            row[1]
            for row in connection.execute(text("PRAGMA table_info(paper_bet_archives)"))
        }
        required_columns = {
            "clv_percent": "ALTER TABLE paper_bet_archives ADD COLUMN clv_percent FLOAT",
            "form_score": "ALTER TABLE paper_bet_archives ADD COLUMN form_score FLOAT",
            "combined_score": "ALTER TABLE paper_bet_archives ADD COLUMN combined_score FLOAT",
            "qualification_reason": "ALTER TABLE paper_bet_archives ADD COLUMN qualification_reason VARCHAR",
            "last_start_finish": "ALTER TABLE paper_bet_archives ADD COLUMN last_start_finish FLOAT",
            "avg_last3_finish": "ALTER TABLE paper_bet_archives ADD COLUMN avg_last3_finish FLOAT",
            "avg_last3_margin": "ALTER TABLE paper_bet_archives ADD COLUMN avg_last3_margin FLOAT",
        }

        for column_name, ddl in required_columns.items():
            if column_name not in columns:
                connection.execute(text(ddl))
