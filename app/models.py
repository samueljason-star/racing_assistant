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
    stake = Column(Float, nullable=False)
    decision_reason = Column(String, nullable=True)
    result = Column(String, nullable=True)
    profit_loss = Column(Float, nullable=True)
    settled_flag = Column(Boolean, default=False)
    decision_version = Column(String, default="v1")
