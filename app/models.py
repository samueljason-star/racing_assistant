from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, ForeignKey
from sqlalchemy.orm import relationship
from app.db import Base
from datetime import datetime


class Meeting(Base):
    __tablename__ = "meetings"

    id = Column(Integer, primary_key=True)
    date = Column(String, nullable=False)
    track = Column(String, nullable=False)
    state = Column(String, nullable=True)
    meeting_type = Column(String, nullable=True)

    races = relationship("Race", back_populates="meeting")


class Race(Base):
    __tablename__ = "races"

    id = Column(Integer, primary_key=True)
    meeting_id = Column(Integer, ForeignKey("meetings.id"))
    betfair_market_id = Column(String, nullable=True)
    race_number = Column(Integer, nullable=False)
    jump_time = Column(String, nullable=True)
    distance = Column(Integer, nullable=True)
    class_name = Column(String, nullable=True)
    track_condition = Column(String, nullable=True)
    field_size = Column(Integer, nullable=True)

    meeting = relationship("Meeting", back_populates="races")
    runners = relationship("Runner", back_populates="race")


class Runner(Base):
    __tablename__ = "runners"

    id = Column(Integer, primary_key=True)
    race_id = Column(Integer, ForeignKey("races.id"))
    horse_name = Column(String, nullable=False)
    barrier = Column(Integer, nullable=True)
    weight = Column(Float, nullable=True)
    jockey = Column(String, nullable=True)
    trainer = Column(String, nullable=True)
    scratching_flag = Column(Boolean, default=False)

    race = relationship("Race", back_populates="runners")


class OddsSnapshot(Base):
    __tablename__ = "odds_snapshots"

    id = Column(Integer, primary_key=True)
    race_id = Column(Integer, nullable=False)
    runner_id = Column(Integer, nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow)
    odds = Column(Float, nullable=False)
    source = Column(String, nullable=True)


class Result(Base):
    __tablename__ = "results"

    id = Column(Integer, primary_key=True)
    race_id = Column(Integer, nullable=False)
    runner_id = Column(Integer, nullable=False)
    finish_position = Column(Integer, nullable=True)
    margin = Column(Float, nullable=True)
    starting_price = Column(Float, nullable=True)


class HorseHistory(Base):
    __tablename__ = "horse_history"

    id = Column(Integer, primary_key=True)
    horse_name = Column(String, nullable=False)
    run_date = Column(String, nullable=True)
    track = Column(String, nullable=True)
    distance = Column(Integer, nullable=True)
    class_name = Column(String, nullable=True)
    barrier = Column(Integer, nullable=True)
    weight = Column(Float, nullable=True)
    jockey = Column(String, nullable=True)
    trainer = Column(String, nullable=True)
    track_condition = Column(String, nullable=True)
    finish_position = Column(Integer, nullable=True)
    margin = Column(Float, nullable=True)
    starting_price = Column(Float, nullable=True)
    source = Column(String, nullable=True, default="results_pipeline")


class Feature(Base):
    __tablename__ = "features"

    id = Column(Integer, primary_key=True)
    race_id = Column(Integer, nullable=False)
    runner_id = Column(Integer, nullable=False)
    market_probability = Column(Float, nullable=True)
    odds_rank = Column(Integer, nullable=True)
    barrier_score = Column(Float, nullable=True)
    form_score = Column(Float, nullable=True)
    trainer_score = Column(Float, nullable=True)
    jockey_score = Column(Float, nullable=True)
    distance_score = Column(Float, nullable=True)
    track_score = Column(Float, nullable=True)
    feature_version = Column(String, default="v1")


class Prediction(Base):
    __tablename__ = "predictions"

    id = Column(Integer, primary_key=True)
    race_id = Column(Integer, nullable=False)
    runner_id = Column(Integer, nullable=False)
    model_probability = Column(Float, nullable=True)
    model_rank = Column(Integer, nullable=True)
    confidence_score = Column(Float, nullable=True)
    model_version = Column(String, default="v1")


class PaperBet(Base):
    __tablename__ = "paper_bets"

    id = Column(Integer, primary_key=True)
    race_id = Column(Integer, nullable=False)
    runner_id = Column(Integer, nullable=False)
    odds_taken = Column(Float, nullable=False)
    market_probability = Column(Float, nullable=False)
    model_probability = Column(Float, nullable=False)
    edge = Column(Float, nullable=False)
    form_score = Column(Float, nullable=True)
    combined_score = Column(Float, nullable=True)
    stake = Column(Float, nullable=False)
    commission_rate = Column(Float, nullable=False, default=0.08)
    qualification_reason = Column(String, nullable=True)
    last_start_finish = Column(Float, nullable=True)
    avg_last3_finish = Column(Float, nullable=True)
    avg_last3_margin = Column(Float, nullable=True)
    decision_reason = Column(String, nullable=True)
    result = Column(String, nullable=True)
    profit_loss = Column(Float, nullable=True)
    settled_flag = Column(Boolean, default=False)
    decision_version = Column(String, default="model_edge_v2")
    paper_bank_reset_id = Column(Integer, ForeignKey("paper_bank_resets.id"), nullable=True)
    closing_odds = Column(Float, nullable=True)
    final_observed_odds = Column(Float, nullable=True)
    closing_line_difference = Column(Float, nullable=True)
    closing_line_pct = Column(Float, nullable=True)
    clv_percent = Column(Float, nullable=True)
    beat_closing_line = Column(Boolean, nullable=True)
    placed_at = Column(DateTime, default=datetime.utcnow)
    settled_at = Column(DateTime, nullable=True)
    proposed_notified_at = Column(DateTime, nullable=True)
    settlement_notified_at = Column(DateTime, nullable=True)


class PaperBankReset(Base):
    __tablename__ = "paper_bank_resets"

    id = Column(Integer, primary_key=True)
    reset_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    decision_version = Column(String, nullable=True)
    baseline_bank = Column(Float, nullable=False, default=10000.0)
    archived_bet_count = Column(Integer, nullable=False, default=0)
    archived_profit_loss = Column(Float, nullable=False, default=0.0)
    note = Column(String, nullable=True)


class PaperBetArchive(Base):
    __tablename__ = "paper_bet_archives"

    id = Column(Integer, primary_key=True)
    original_paper_bet_id = Column(Integer, nullable=False)
    race_id = Column(Integer, nullable=False)
    runner_id = Column(Integer, nullable=False)
    odds_taken = Column(Float, nullable=False)
    market_probability = Column(Float, nullable=False)
    model_probability = Column(Float, nullable=False)
    edge = Column(Float, nullable=False)
    form_score = Column(Float, nullable=True)
    combined_score = Column(Float, nullable=True)
    stake = Column(Float, nullable=False)
    commission_rate = Column(Float, nullable=False, default=0.08)
    qualification_reason = Column(String, nullable=True)
    last_start_finish = Column(Float, nullable=True)
    avg_last3_finish = Column(Float, nullable=True)
    avg_last3_margin = Column(Float, nullable=True)
    decision_reason = Column(String, nullable=True)
    result = Column(String, nullable=True)
    profit_loss = Column(Float, nullable=True)
    settled_flag = Column(Boolean, default=False)
    decision_version = Column(String, default="model_edge_v2")
    paper_bank_reset_id = Column(Integer, nullable=True)
    closing_odds = Column(Float, nullable=True)
    final_observed_odds = Column(Float, nullable=True)
    closing_line_difference = Column(Float, nullable=True)
    closing_line_pct = Column(Float, nullable=True)
    clv_percent = Column(Float, nullable=True)
    beat_closing_line = Column(Boolean, nullable=True)
    placed_at = Column(DateTime, nullable=True)
    settled_at = Column(DateTime, nullable=True)
    proposed_notified_at = Column(DateTime, nullable=True)
    settlement_notified_at = Column(DateTime, nullable=True)
    archived_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    archived_reason = Column(String, nullable=True)
