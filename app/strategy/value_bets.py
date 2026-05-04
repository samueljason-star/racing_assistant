from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from app.betting.bet_details import enrich_paper_bets
from app.betting.market_helpers import calculate_edge, commission_adjusted_market_probability, raw_market_probability
from app.betting.paper_bank import get_latest_reset, get_strategy_bank, get_strategy_next_stake
from app.config import (
    ACTIVE_DECISION_VERSION,
    BETFAIR_COMMISSION_RATE,
    PAPER_MAX_MODEL_PROBABILITY,
    PAPER_MAX_ODDS,
    PAPER_MIN_EDGE,
    PAPER_MIN_ODDS,
)
from app.db import SessionLocal, init_db
from app.models import Feature, HorseHistory, OddsSnapshot, PaperBet, Prediction, Race, Runner
from app.notifier.telegram import send_telegram_message
from app.utils.name_matching import horse_names_match, normalize_horse_name

BRISBANE_TZ = ZoneInfo("Australia/Brisbane")
ROOT_DIR = Path(__file__).resolve().parents[2]
MODEL_EDGE_V3_CANDIDATE_PATH = ROOT_DIR / "app" / "research" / "artifacts" / "model_edge_v3_candidate.json"
MIN_FIELD_SIZE = 6
COMMISSION_RATE = BETFAIR_COMMISSION_RATE
DECISION_VERSION = ACTIVE_DECISION_VERSION
LATE_MARKET_MINUTES = 75

DEFAULT_STRATEGY_PROFILES = {
    "model_edge_v2": {
        "decision_version": "model_edge_v2",
        "min_runner_odds": PAPER_MIN_ODDS,
        "max_runner_odds": PAPER_MAX_ODDS,
        "min_edge": PAPER_MIN_EDGE,
        "max_model_probability": PAPER_MAX_MODEL_PROBABILITY,
        "max_daily_bets": 5,
        "min_form_score": 0.30,
        "proposed_header": "PROPOSED BET",
        "watchlist_header": "WATCHLIST ONLY - NO HISTORY",
        "mode": "legacy_v2",
        "validation_tier": "legacy_v2",
    },
    "model_edge_v3": {
        "decision_version": "model_edge_v3",
        "min_runner_odds": PAPER_MIN_ODDS,
        "max_runner_odds": 20.0,
        "min_edge": max(PAPER_MIN_EDGE, 0.0),
        "max_model_probability": PAPER_MAX_MODEL_PROBABILITY,
        "max_daily_bets": 3,
        "min_form_score": 0.30,
        "proposed_header": "PROPOSED BET — model_edge_v3",
        "watchlist_header": "WATCHLIST ONLY — NO BET PLACED",
        "mode": "morning_model",
        "validation_tier": "balanced",
    },
}


def _load_strategy_profile() -> dict[str, object]:
    profile = dict(DEFAULT_STRATEGY_PROFILES.get(DECISION_VERSION, DEFAULT_STRATEGY_PROFILES["model_edge_v2"]))
    if DECISION_VERSION != "model_edge_v3":
        return profile
    if not MODEL_EDGE_V3_CANDIDATE_PATH.exists():
        profile["disabled_reason"] = f"missing_validation_artifact:{MODEL_EDGE_V3_CANDIDATE_PATH}"
        return profile
    try:
        payload = json.loads(MODEL_EDGE_V3_CANDIDATE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        profile["disabled_reason"] = f"invalid_validation_artifact:{MODEL_EDGE_V3_CANDIDATE_PATH}"
        return profile

    if bool(payload.get("disabled")):
        profile["disabled_reason"] = payload.get("disabled_reason", "validation_disabled")
        return profile

    profile.update(
        {
            "max_runner_odds": min(float(payload.get("max_odds", 20.0)), 30.0),
            "min_edge": max(float(payload.get("min_edge", 0.0)), 0.0),
            "max_daily_bets": int(payload.get("max_bets_per_day", 3)),
            "min_form_score": max(float(payload.get("min_form_score", 0.30)), 0.30),
            "mode": payload.get("live_mode", "morning_model"),
            "validation_tier": payload.get("source_tier", "balanced"),
            "source_model_name": payload.get("source_model_name", "logistic"),
            "warnings": payload.get("warnings", []),
        }
    )
    return profile


def race_already_has_bet(db, race_id, decision_version: str):
    existing_bet = (
        db.query(PaperBet)
        .filter(PaperBet.race_id == race_id)
        .filter(PaperBet.decision_version == decision_version)
        .first()
    )
    return existing_bet is not None


def _parse_jump_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _is_upcoming_race(race) -> bool:
    jump_time = _parse_jump_time(race.jump_time)
    if not jump_time:
        return False
    return jump_time > datetime.now(timezone.utc)


def _minutes_to_jump(race) -> float | None:
    jump_time = _parse_jump_time(race.jump_time)
    if not jump_time:
        return None
    return round((jump_time - datetime.now(timezone.utc)).total_seconds() / 60.0, 2)


def _mode_for_race(race, profile: dict[str, object]) -> str:
    if profile.get("decision_version") != "model_edge_v3":
        return str(profile.get("mode", "legacy_v2"))
    minutes_to_jump = _minutes_to_jump(race)
    if minutes_to_jump is not None and minutes_to_jump <= LATE_MARKET_MINUTES:
        return "late_market_model"
    return "morning_model"


def get_latest_odds(db, runner_id):
    snapshot = (
        db.query(OddsSnapshot)
        .filter(OddsSnapshot.runner_id == runner_id)
        .order_by(OddsSnapshot.timestamp.desc())
        .first()
    )
    if not snapshot or snapshot.odds is None or snapshot.odds <= 0:
        return None
    return snapshot.odds


def _safe_finish_value(value) -> float | None:
    if value is None or value <= 0:
        return None
    return float(value)


def _stripped_runner_name(horse_name: str) -> str:
    return re.sub(r"^\d+\s*[\.\-]?\s*", "", (horse_name or "").strip()).strip()


def _recent_history_rows(db, horse_name: str):
    stripped_name = _stripped_runner_name(horse_name)
    exact_aliases = {
        value
        for value in {horse_name.strip(), stripped_name}
        if value
    }

    exact_rows = (
        db.query(HorseHistory)
        .filter(HorseHistory.horse_name.in_(sorted(exact_aliases)))
        .filter(HorseHistory.source != "results_pipeline")
        .order_by(HorseHistory.id.desc())
        .limit(10)
        .all()
    )
    if exact_rows:
        return exact_rows[:3]

    first_token = stripped_name.split()[0] if stripped_name else ""
    if not first_token:
        return []

    candidate_rows = (
        db.query(HorseHistory)
        .filter(HorseHistory.source != "results_pipeline")
        .filter(HorseHistory.horse_name.ilike(f"%{first_token}%"))
        .order_by(HorseHistory.id.desc())
        .limit(50)
        .all()
    )

    matched_rows = []
    seen_ids = set()
    for row in candidate_rows:
        if row.id in seen_ids:
            continue
        if horse_names_match(row.horse_name, stripped_name):
            matched_rows.append(row)
            seen_ids.add(row.id)
        if len(matched_rows) >= 3:
            break

    return matched_rows


def _build_recent_form(rows):
    if not rows:
        return {
            "has_history": False,
            "history_row_count": 0,
            "qualifies": False,
            "qualification_reason": "no_history",
            "form_score": 0.0,
            "last_start_finish": None,
            "avg_last3_finish": None,
            "avg_last3_margin": None,
            "has_recent_placing": False,
        }

    finish_positions = [
        _safe_finish_value(row.finish_position)
        for row in rows
        if _safe_finish_value(row.finish_position) is not None
    ]
    margins = [
        float(row.margin)
        for row in rows
        if row.margin is not None
    ]
    last_start_finish = _safe_finish_value(rows[0].finish_position)
    avg_last3_finish = sum(finish_positions) / len(finish_positions) if finish_positions else None
    avg_last3_margin = sum(margins) / len(margins) if margins else None
    has_recent_placing = any(
        _safe_finish_value(row.finish_position) is not None and _safe_finish_value(row.finish_position) <= 3
        for row in rows
    )
    has_recent_margin = any(
        row.margin is not None and float(row.margin) <= 3.0
        for row in rows
    )

    reasons = []
    if avg_last3_finish is not None and avg_last3_finish <= 6:
        reasons.append("avg_finish<=6")
    if last_start_finish is not None and last_start_finish <= 4:
        reasons.append("last_start<=4")
    if has_recent_margin:
        reasons.append("recent_margin<=3")
    if has_recent_placing:
        reasons.append("recent_placing")

    finish_component = 0.0 if avg_last3_finish is None else max(0.0, min(1.0, (10.0 - avg_last3_finish) / 9.0))
    last_start_component = 0.0 if last_start_finish is None else max(0.0, min(1.0, (10.0 - last_start_finish) / 9.0))
    margin_component = 0.0 if avg_last3_margin is None else max(0.0, min(1.0, (6.0 - avg_last3_margin) / 6.0))
    placing_bonus = 0.2 if has_recent_placing else 0.0

    form_score = min(
        1.0,
        round((last_start_component * 0.45) + (finish_component * 0.35) + (margin_component * 0.20) + placing_bonus, 4),
    )

    return {
        "has_history": True,
        "history_row_count": len(rows),
        "qualifies": bool(reasons),
        "qualification_reason": ", ".join(reasons) if reasons else "poor_recent_form",
        "form_score": form_score,
        "last_start_finish": last_start_finish,
        "avg_last3_finish": round(avg_last3_finish, 2) if avg_last3_finish is not None else None,
        "avg_last3_margin": round(avg_last3_margin, 2) if avg_last3_margin is not None else None,
        "has_recent_placing": has_recent_placing,
        "has_recent_margin": has_recent_margin,
    }


def build_runner_signal(db, runner, profile: dict[str, object], strategy_mode: str):
    latest_odds = get_latest_odds(db, runner.id)
    if latest_odds is None:
        return None, "missing_odds", None

    prediction = db.query(Prediction).filter(
        Prediction.race_id == runner.race_id,
        Prediction.runner_id == runner.id,
    ).first()
    if not prediction or prediction.model_probability is None:
        return None, "missing_prediction", None

    if prediction.model_probability > float(profile["max_model_probability"]):
        return None, "model_probability_cap", None

    feature = db.query(Feature).filter(
        Feature.race_id == runner.race_id,
        Feature.runner_id == runner.id,
    ).first()

    raw_probability = raw_market_probability(latest_odds)
    market_probability = commission_adjusted_market_probability(
        latest_odds,
        COMMISSION_RATE,
    )
    model_probability = prediction.model_probability
    edge = calculate_edge(model_probability, market_probability)

    if market_probability is None or edge is None:
        return None, "invalid_market_probability", None

    if latest_odds < float(profile["min_runner_odds"]):
        return None, "odds_band", None

    if latest_odds > float(profile["max_runner_odds"]):
        if profile.get("decision_version") == "model_edge_v3":
            return None, "watchlist_odds_cap", {
                "runner": runner,
                "latest_odds": latest_odds,
                "market_probability": market_probability,
                "model_probability": model_probability,
                "edge": edge,
                "reason": "watchlist_above_live_odds_cap",
                "strategy_mode": strategy_mode,
            }
        return None, "odds_band", None

    if edge < float(profile["min_edge"]):
        return None, "edge_threshold", None

    recent_form = _build_recent_form(_recent_history_rows(db, runner.horse_name))
    if not recent_form["has_history"] or recent_form["history_row_count"] < 1:
        return None, "missing_form_history", {
            "runner": runner,
            "latest_odds": latest_odds,
            "market_probability": market_probability,
            "model_probability": model_probability,
            "edge": edge,
            "reason": "watchlist_no_history",
        }
    if not recent_form["qualifies"]:
        return None, "poor_recent_form", None
    if recent_form["form_score"] <= 0 or recent_form["form_score"] < float(profile["min_form_score"]):
        return None, "form_score_too_low", None

    qualification_reason = recent_form["qualification_reason"]
    form_score = recent_form["form_score"]
    combined_score = round((edge * 0.65) + (form_score * 0.35), 4)

    return {
        "runner": runner,
        "feature": feature,
        "prediction": prediction,
        "latest_odds": latest_odds,
        "raw_market_probability": raw_probability,
        "market_probability": market_probability,
        "model_probability": model_probability,
        "edge": edge,
        "form_score": form_score,
        "combined_score": combined_score,
        "qualification_reason": qualification_reason,
        "last_start_finish": recent_form["last_start_finish"],
        "avg_last3_finish": recent_form["avg_last3_finish"],
        "avg_last3_margin": recent_form["avg_last3_margin"],
        "history_row_count": recent_form["history_row_count"],
        "strategy_mode": strategy_mode,
        "validation_tier": profile.get("validation_tier"),
        "source_model_name": profile.get("source_model_name"),
    }, None, None


def get_daily_bet_count(db, decision_version: str) -> int:
    now_brisbane = datetime.now(BRISBANE_TZ)
    start_local = datetime.combine(now_brisbane.date(), datetime.min.time(), tzinfo=BRISBANE_TZ)
    end_local = start_local + timedelta(days=1)
    start_utc = start_local.astimezone(timezone.utc).replace(tzinfo=None)
    end_utc = end_local.astimezone(timezone.utc).replace(tzinfo=None)

    return (
        db.query(PaperBet)
        .filter(PaperBet.decision_version == decision_version)
        .filter(PaperBet.placed_at >= start_utc, PaperBet.placed_at < end_utc)
        .count()
    )


def get_race_candidates(db, race, counters, all_candidates, profile: dict[str, object]):
    runners = db.query(Runner).filter(Runner.race_id == race.id).all()
    if len(runners) < MIN_FIELD_SIZE:
        counters["races_skipped_field_size"] += 1
        return None, [], []

    candidates = []
    watchlist_candidates = []
    strategy_mode = _mode_for_race(race, profile)
    for runner in runners:
        signal, skip_reason, extra = build_runner_signal(db, runner, profile, strategy_mode)
        if not signal:
            key = f"runners_skipped_{skip_reason}"
            if key in counters:
                counters[key] += 1
            if skip_reason in {"missing_form_history", "watchlist_odds_cap"} and extra:
                watchlist_candidates.append(extra)
                if skip_reason == "missing_form_history":
                    counters["runners_watchlist_no_history"] += 1
                else:
                    counters["runners_watchlist_odds_cap"] += 1
            continue

        all_candidates.append(signal)
        candidates.append(signal)

    candidates.sort(key=lambda item: item["combined_score"], reverse=True)
    return len(runners), candidates, watchlist_candidates


def create_paper_bet(db, race, chosen_runner, stake, profile: dict[str, object]):
    decision_version = str(profile["decision_version"])
    latest_reset = get_latest_reset(db, decision_version)
    paper_bet = PaperBet(
        race_id=race.id,
        runner_id=chosen_runner["runner"].id,
        odds_taken=chosen_runner["latest_odds"],
        market_probability=chosen_runner["market_probability"],
        model_probability=chosen_runner["model_probability"],
        edge=chosen_runner["edge"],
        form_score=chosen_runner["form_score"],
        combined_score=chosen_runner["combined_score"],
        qualification_reason=chosen_runner["qualification_reason"],
        last_start_finish=chosen_runner["last_start_finish"],
        avg_last3_finish=chosen_runner["avg_last3_finish"],
        avg_last3_margin=chosen_runner["avg_last3_margin"],
        stake=stake,
        commission_rate=COMMISSION_RATE,
        decision_reason=(
            f"edge={chosen_runner['edge']:.4f} | "
            f"form_score={chosen_runner['form_score']:.4f} | "
            f"combined_score={chosen_runner['combined_score']:.4f} | "
            f"recent_form_reason={chosen_runner['qualification_reason']} | "
            f"strategy_mode={chosen_runner.get('strategy_mode', profile.get('mode'))} | "
            f"validation_tier={chosen_runner.get('validation_tier', profile.get('validation_tier'))} | "
            f"source_model={chosen_runner.get('source_model_name', profile.get('source_model_name', 'n/a'))} | "
            f"last_start_finish={chosen_runner['last_start_finish']} | "
            f"avg_last3_finish={chosen_runner['avg_last3_finish']} | "
            f"avg_margin={chosen_runner['avg_last3_margin']} | "
            f"odds_taken={chosen_runner['latest_odds']:.2f}"
        ),
        result=None,
        profit_loss=None,
        settled_flag=False,
        decision_version=decision_version,
        paper_bank_reset_id=latest_reset.id if latest_reset else None,
        proposed_notified_at=None,
        settlement_notified_at=None,
    )
    db.add(paper_bet)
    return paper_bet


def _send_proposed_notification(db, bet: PaperBet, profile: dict[str, object]) -> bool:
    bet_detail = enrich_paper_bets(db, [bet])[0]
    if (
        (bet_detail["form_score"] or 0.0) <= 0.0
        or (bet_detail["qualification_reason"] or "").strip().lower() == "strong_edge_no_history"
    ):
        return False
    strategy_bank = get_strategy_bank(db, bet.decision_version or str(profile["decision_version"]))
    header = str(profile.get("proposed_header", "PROPOSED BET"))
    strategy_mode = "late_market_model" if "strategy_mode=late_market_model" in (bet.decision_reason or "") else "morning_model"
    message = (
        f"{header}\n"
        f"Horse: {bet_detail['horse_name']}\n"
        f"Track/Race: {bet_detail['track'] or 'Unknown'} R{bet_detail['race_number'] or '?'}\n"
        f"Race Time: {bet_detail['jump_time'] or 'Unknown'}\n"
        f"Race ID: {bet_detail['race_id']}\n"
        f"Odds Taken: {bet_detail['odds_taken']:.2f}\n"
        f"Stake: ${bet_detail['stake']:.2f}\n"
        f"Model Probability: {bet_detail['model_probability']:.4f}\n"
        f"Adj Market Probability: {bet_detail['market_probability']:.4f}\n"
        f"Edge: {bet_detail['edge']:.4f}\n"
        f"Form Score: {(bet_detail['form_score'] or 0.0):.4f}\n"
        f"Combined Score: {(bet_detail['combined_score'] or 0.0):.4f}\n"
        f"Recent Form: {bet_detail['qualification_reason'] or 'N/A'}\n"
        f"Mode: {strategy_mode}\n"
        f"Version: {bet.decision_version or str(profile['decision_version'])}\n"
        f"Strategy Bank: ${strategy_bank:.2f}"
    )
    if send_telegram_message(message):
        bet.proposed_notified_at = datetime.utcnow()
        return True
    return False


def _notify_unsent_proposed_bets(db, profile: dict[str, object]) -> int:
    decision_version = str(profile["decision_version"])
    unsent_bets = (
        db.query(PaperBet)
        .filter(PaperBet.decision_version == decision_version)
        .filter(PaperBet.settled_flag == False)
        .filter(PaperBet.proposed_notified_at.is_(None))
        .order_by(PaperBet.id.asc())
        .all()
    )
    sent_count = 0
    for bet in unsent_bets:
        if _send_proposed_notification(db, bet, profile):
            sent_count += 1
    return sent_count


def _send_watchlist_notification(profile: dict[str, object], watchlist_candidates: list[dict[str, object]]) -> bool:
    if not watchlist_candidates or profile.get("decision_version") != "model_edge_v3":
        return False

    lines = [str(profile.get("watchlist_header", "WATCHLIST ONLY - NO BET PLACED"))]
    for candidate in sorted(watchlist_candidates, key=lambda item: item["edge"], reverse=True)[:5]:
        lines.append(
            f"{candidate['runner'].horse_name} | "
            f"race_id={candidate['runner'].race_id} | "
            f"odds={candidate['latest_odds']:.2f} | "
            f"edge={candidate['edge']:.4f} | "
            f"mode={candidate.get('strategy_mode', profile.get('mode', 'morning_model'))} | "
            f"reason={candidate['reason']}"
        )
    return send_telegram_message("\n".join(lines))


def create_value_bets():
    init_db()
    db = SessionLocal()

    try:
        profile = _load_strategy_profile()
        if profile.get("disabled_reason"):
            print(f"STRATEGY {DECISION_VERSION} DISABLED: {profile['disabled_reason']}")
            return

        races = db.query(Race).filter(Race.betfair_market_id.isnot(None)).all()
        current_bank = get_strategy_bank(db, str(profile["decision_version"]))
        existing_daily_bets = get_daily_bet_count(db, str(profile["decision_version"]))
        remaining_daily_bets = max(0, int(profile["max_daily_bets"]) - existing_daily_bets)

        races_checked = 0
        candidates_found = 0
        bets_created = 0
        created_edges = []
        counters = {
            "races_skipped_field_size": 0,
            "runners_skipped_missing_prediction": 0,
            "runners_skipped_missing_odds": 0,
            "runners_skipped_invalid_market_probability": 0,
            "runners_skipped_model_probability_cap": 0,
            "runners_skipped_odds_band": 0,
            "runners_skipped_edge_threshold": 0,
            "runners_skipped_missing_form_history": 0,
            "runners_skipped_form_score_too_low": 0,
            "runners_skipped_poor_recent_form": 0,
            "runners_skipped_watchlist_odds_cap": 0,
            "runners_skipped_daily_bet_cap": 0,
            "runners_watchlist_no_history": 0,
            "runners_watchlist_odds_cap": 0,
        }
        all_candidates = []
        race_best_candidates = []
        watchlist_candidates = []

        for race in races:
            if not _is_upcoming_race(race):
                continue
            races_checked += 1

            if race_already_has_bet(db, race.id, str(profile["decision_version"])):
                continue

            field_size, candidates, race_watchlist = get_race_candidates(db, race, counters, all_candidates, profile)
            if field_size is None or not candidates:
                watchlist_candidates.extend(race_watchlist)
                continue

            watchlist_candidates.extend(race_watchlist)
            best_candidate = candidates[0]
            race_best_candidates.append(best_candidate)
            candidates_found += 1

        race_best_candidates.sort(key=lambda item: item["combined_score"], reverse=True)
        selected_candidates = race_best_candidates[:remaining_daily_bets]
        counters["runners_skipped_daily_bet_cap"] = max(0, len(race_best_candidates) - len(selected_candidates))

        for chosen in selected_candidates:
            stake = get_strategy_next_stake(db, str(profile["decision_version"]))
            race = chosen["runner"].race
            create_paper_bet(db, race, chosen, stake, profile)
            bets_created += 1
            created_edges.append(chosen["edge"])

        db.commit()
        proposed_notifications_sent = _notify_unsent_proposed_bets(db, profile)
        watchlist_notification_sent = _send_watchlist_notification(profile, watchlist_candidates)
        db.commit()

        avg_edge = sum(created_edges) / len(created_edges) if created_edges else 0.0
        top_candidates = sorted(
            all_candidates,
            key=lambda item: item["combined_score"],
            reverse=True,
        )[:20]

        print(f"STRATEGY BANK {profile['decision_version']}: ${current_bank:.2f}")
        print(f"RACES CHECKED: {races_checked}")
        print(f"CANDIDATES FOUND: {candidates_found}")
        print(f"PAPER BETS CREATED: {bets_created}")
        print(f"PROPOSED BET NOTIFICATIONS SENT: {proposed_notifications_sent}")
        print(f"WATCHLIST NOTIFICATION SENT: {watchlist_notification_sent}")
        print(f"AVG EDGE OF CREATED BETS: {avg_edge:.4f}")
        print("TOP 20 CANDIDATES BY COMBINED SCORE")
        for candidate in top_candidates:
            print(
                f"race_id={candidate['runner'].race_id} | "
                f"horse={candidate['runner'].horse_name} | "
                f"odds={candidate['latest_odds']:.2f} | "
                f"model_probability={candidate['model_probability']:.4f} | "
                f"market_probability_adj={candidate['market_probability']:.4f} | "
                f"edge={candidate['edge']:.4f} | "
                f"form_score={candidate['form_score']:.4f} | "
                f"combined_score={candidate['combined_score']:.4f} | "
                f"mode={candidate.get('strategy_mode', profile.get('mode'))} | "
                f"reason={candidate['qualification_reason']}"
            )
        if watchlist_candidates:
            print(profile.get("watchlist_header", "WATCHLIST ONLY - NO BET PLACED"))
            for candidate in sorted(watchlist_candidates, key=lambda item: item["edge"], reverse=True)[:20]:
                print(
                    f"race_id={candidate['runner'].race_id} | "
                    f"horse={candidate['runner'].horse_name} | "
                    f"odds={candidate['latest_odds']:.2f} | "
                    f"model_probability={candidate['model_probability']:.4f} | "
                    f"market_probability_adj={candidate['market_probability']:.4f} | "
                    f"edge={candidate['edge']:.4f} | "
                    f"mode={candidate.get('strategy_mode', profile.get('mode'))} | "
                    f"reason={candidate['reason']}"
                )
        print(f"RUNNERS SKIPPED DUE TO MISSING PREDICTION: {counters['runners_skipped_missing_prediction']}")
        print(f"RUNNERS SKIPPED DUE TO MISSING ODDS: {counters['runners_skipped_missing_odds']}")
        print(f"RUNNERS SKIPPED DUE TO ODDS BAND: {counters['runners_skipped_odds_band']}")
        print(f"RUNNERS SKIPPED DUE TO EDGE THRESHOLD: {counters['runners_skipped_edge_threshold']}")
        print(f"RUNNERS SKIPPED DUE TO MODEL PROBABILITY CAP: {counters['runners_skipped_model_probability_cap']}")
        print(f"RUNNERS SKIPPED DUE TO MISSING FORM HISTORY: {counters['runners_skipped_missing_form_history']}")
        print(f"RUNNERS SKIPPED DUE TO FORM SCORE TOO LOW: {counters['runners_skipped_form_score_too_low']}")
        print(f"RUNNERS SKIPPED DUE TO POOR RECENT FORM: {counters['runners_skipped_poor_recent_form']}")
        print(f"WATCHLIST ONLY - NO HISTORY: {counters['runners_watchlist_no_history']}")
        print(f"WATCHLIST ONLY - ABOVE V3 ODDS CAP: {counters['runners_watchlist_odds_cap']}")
        print(f"RUNNERS SKIPPED DUE TO DAILY BET CAP: {counters['runners_skipped_daily_bet_cap']}")
    finally:
        db.close()


if __name__ == "__main__":
    create_value_bets()
