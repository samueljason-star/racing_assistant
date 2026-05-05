from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from app.betting.bet_details import enrich_paper_bets
from app.betting.market_helpers import calculate_edge, commission_adjusted_market_probability
from app.betting.paper_bank import get_latest_reset, get_strategy_bank, get_strategy_next_stake
from app.config import BETFAIR_COMMISSION_RATE, PAPER_MAX_MODEL_PROBABILITY
from app.db import SessionLocal, init_db
from app.models import OddsSnapshot, PaperBet, Prediction, Race, Runner
from app.notifier.telegram import send_telegram_message
from app.strategy.value_bets import _build_recent_form, _parse_jump_time, _recent_history_rows

BRISBANE_TZ = ZoneInfo("Australia/Brisbane")
DECISION_VERSION = "model_edge_late_v2"
COMMISSION_RATE = BETFAIR_COMMISSION_RATE
MAX_MODEL_PROBABILITY = PAPER_MAX_MODEL_PROBABILITY
MIN_FORM_SCORE = 0.30
MIN_RUNNER_ODDS = 3.0
MAX_RUNNER_ODDS = 20.0
WATCHLIST_MIN_ODDS = 20.0
WATCHLIST_MAX_ODDS = 50.0
MAX_MARKET_RANK = 8
MINUTES_TO_JUMP_MIN = 1.0
MINUTES_TO_JUMP_MAX = 3.0
MAX_DAILY_BETS = 3
MIN_EDGE = -0.01
MIN_MOVEMENT_SCORE = 0.45
MIN_COMBINED_SCORE = 0.45
WATCHLIST_MOVEMENT_SCORE = 0.60
MAX_REJECTION_LOG_ROWS = 5


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _minutes_to_jump(race) -> float | None:
    jump_time = _parse_jump_time(race.jump_time)
    if not jump_time:
        return None
    return round((jump_time - _now_utc()).total_seconds() / 60.0, 2)


def _in_late_window(race) -> tuple[bool, float | None]:
    minutes = _minutes_to_jump(race)
    if minutes is None:
        return False, None
    return MINUTES_TO_JUMP_MIN <= minutes <= MINUTES_TO_JUMP_MAX, minutes


def _race_already_has_bet(db, race_id: int) -> bool:
    return (
        db.query(PaperBet)
        .filter(PaperBet.race_id == race_id)
        .filter(PaperBet.decision_version == DECISION_VERSION)
        .first()
        is not None
    )


def _get_runner_snapshots(db, runner_id: int) -> list[OddsSnapshot]:
    return (
        db.query(OddsSnapshot)
        .filter(OddsSnapshot.runner_id == runner_id)
        .order_by(OddsSnapshot.timestamp.asc())
        .all()
    )


def _snapshot_before(snapshots: list[OddsSnapshot], cutoff: datetime) -> OddsSnapshot | None:
    chosen = None
    for snapshot in snapshots:
        ts = snapshot.timestamp
        if ts is None:
            continue
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        if ts <= cutoff:
            chosen = snapshot
        else:
            break
    return chosen


def _safe_odds(snapshot: OddsSnapshot | None) -> float | None:
    if not snapshot or snapshot.odds is None or snapshot.odds <= 0:
        return None
    return float(snapshot.odds)


def _safe_timestamp(snapshot: OddsSnapshot | None) -> datetime | None:
    if not snapshot or snapshot.timestamp is None:
        return None
    ts = snapshot.timestamp
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts


def _format_timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(BRISBANE_TZ).strftime("%H:%M:%S")


def _runner_market_snapshot(db, runner: Runner) -> dict[str, object] | None:
    evaluation_time = _now_utc()
    snapshots = _get_runner_snapshots(db, runner.id)
    if not snapshots:
        return None

    usable = []
    for snapshot in snapshots:
        ts = _safe_timestamp(snapshot)
        if ts is not None and ts <= evaluation_time:
            usable.append(snapshot)

    if not usable:
        return None

    latest = usable[-1]
    cut_10 = _snapshot_before(usable, evaluation_time - timedelta(minutes=10))
    cut_5 = _snapshot_before(usable, evaluation_time - timedelta(minutes=5))
    cut_3 = _snapshot_before(usable, evaluation_time - timedelta(minutes=3))
    cut_1 = _snapshot_before(usable, evaluation_time - timedelta(minutes=1))

    latest_odds = _safe_odds(latest)
    if latest_odds is None:
        return None

    odds_10 = _safe_odds(cut_10)
    odds_5 = _safe_odds(cut_5)
    odds_3 = _safe_odds(cut_3)
    odds_1 = _safe_odds(cut_1)

    return {
        "latest_odds": latest_odds,
        "latest_odds_timestamp": _safe_timestamp(latest),
        "odds_10m": odds_10,
        "odds_5m": odds_5,
        "odds_3m": odds_3,
        "odds_1m": odds_1,
        "movement_10_to_now": (odds_10 - latest_odds) if odds_10 is not None else None,
        "movement_5_to_now": (odds_5 - latest_odds) if odds_5 is not None else None,
        "movement_3_to_now": (odds_3 - latest_odds) if odds_3 is not None else None,
        "movement_1_to_now": (odds_1 - latest_odds) if odds_1 is not None else None,
    }


def _race_market_ranks(snapshot_by_runner: dict[int, dict[str, object]]) -> dict[int, int]:
    ranked = [
        (runner_id, values["latest_odds"])
        for runner_id, values in snapshot_by_runner.items()
        if values.get("latest_odds") is not None
    ]
    ranked.sort(key=lambda item: item[1])
    rank_map: dict[int, int] = {}
    rank = 0
    last_odds = None
    for runner_id, odds in ranked:
        if last_odds is None or odds != last_odds:
            rank += 1
            last_odds = odds
        rank_map[runner_id] = rank
    return rank_map


def _movement_metrics(market_snapshot: dict[str, object]) -> dict[str, object]:
    movements = {
        "movement_10_to_now": market_snapshot.get("movement_10_to_now"),
        "movement_5_to_now": market_snapshot.get("movement_5_to_now"),
        "movement_3_to_now": market_snapshot.get("movement_3_to_now"),
        "movement_1_to_now": market_snapshot.get("movement_1_to_now"),
    }
    available = [value for value in movements.values() if value is not None]
    if not available:
        return {
            "has_movement": False,
            "skip_reason": "missing_late_movement",
            "movement_score": 0.0,
            "best_movement": None,
            "recent_drift": False,
        }

    positive_count = sum(1 for value in available if value > 0)
    recent_1 = movements["movement_1_to_now"]
    recent_3 = movements["movement_3_to_now"]
    recent_5 = movements["movement_5_to_now"]
    recent_positive_count = sum(
        1 for value in (recent_5, recent_3, recent_1) if value is not None and value > 0
    )
    recent_drift = (
        (recent_1 is not None and recent_1 < 0)
        or (recent_3 is not None and recent_3 < 0 and (recent_1 is None or recent_1 <= 0))
        or (positive_count == 1 and (movements["movement_10_to_now"] or 0) > 0 and recent_positive_count == 0)
    )

    weighted_score = 0.0
    total_weight = 0.0
    for key, weight in (
        ("movement_10_to_now", 0.15),
        ("movement_5_to_now", 0.25),
        ("movement_3_to_now", 0.30),
        ("movement_1_to_now", 0.30),
    ):
        movement = movements[key]
        if movement is None:
            continue
        total_weight += weight
        if movement > 0:
            component = min(movement / 2.0, 1.0)
        elif movement == 0:
            component = 0.20
        else:
            component = max(0.0, 0.15 + movement)
        weighted_score += component * weight

    movement_score = weighted_score / total_weight if total_weight else 0.0
    if positive_count >= 3:
        movement_score += 0.12
    elif positive_count == 2:
        movement_score += 0.07
    if (recent_1 or 0) > 0 and (recent_3 or 0) > 0:
        movement_score += 0.08
    if recent_drift:
        movement_score -= 0.30
    movement_score = round(max(0.0, min(1.0, movement_score)), 4)

    return {
        "has_movement": True,
        "skip_reason": "recent_drift" if recent_drift else None,
        "movement_score": movement_score,
        "best_movement": round(max(available), 4),
        "recent_drift": recent_drift,
    }


def _daily_bet_count(db) -> int:
    now_brisbane = datetime.now(BRISBANE_TZ)
    start_local = datetime.combine(now_brisbane.date(), datetime.min.time(), tzinfo=BRISBANE_TZ)
    end_local = start_local + timedelta(days=1)
    start_utc = start_local.astimezone(timezone.utc).replace(tzinfo=None)
    end_utc = end_local.astimezone(timezone.utc).replace(tzinfo=None)
    return (
        db.query(PaperBet)
        .filter(PaperBet.decision_version == DECISION_VERSION)
        .filter(PaperBet.placed_at >= start_utc, PaperBet.placed_at < end_utc)
        .count()
    )


def _signal_scores(movement_score: float, edge: float | None, form_score: float) -> float:
    edge_component = max(edge if edge is not None else -0.05, -0.05)
    return round((movement_score * 0.55) + (form_score * 0.30) + (edge_component * 0.15), 4)


def _build_rejection_log(
    db,
    race,
    runner: Runner,
    market_snapshot: dict[str, object] | None,
    market_rank: int | None,
    skip_reason: str,
    minutes_to_jump: float | None,
) -> dict[str, object]:
    meeting = getattr(race, "meeting", None)
    prediction = (
        db.query(Prediction)
        .filter(Prediction.race_id == runner.race_id, Prediction.runner_id == runner.id)
        .first()
    )
    latest_odds = None if not market_snapshot else market_snapshot.get("latest_odds")
    model_probability = prediction.model_probability if prediction and prediction.model_probability is not None else None
    market_probability = (
        commission_adjusted_market_probability(latest_odds, COMMISSION_RATE) if latest_odds is not None else None
    )
    edge = (
        calculate_edge(model_probability, market_probability)
        if model_probability is not None and market_probability is not None
        else None
    )
    recent_form = _build_recent_form(_recent_history_rows(db, runner.horse_name))
    movement_metrics = _movement_metrics(market_snapshot or {})
    movement_score = movement_metrics["movement_score"]
    form_score = recent_form.get("form_score") or 0.0
    combined_score = _signal_scores(movement_score, edge, form_score)

    return {
        "track": meeting.track if meeting else None,
        "race_number": race.race_number,
        "horse_name": runner.horse_name,
        "minutes_to_jump": minutes_to_jump,
        "latest_odds": latest_odds,
        "odds_10m": None if not market_snapshot else market_snapshot.get("odds_10m"),
        "odds_5m": None if not market_snapshot else market_snapshot.get("odds_5m"),
        "odds_3m": None if not market_snapshot else market_snapshot.get("odds_3m"),
        "odds_1m": None if not market_snapshot else market_snapshot.get("odds_1m"),
        "latest_odds_timestamp": None if not market_snapshot else market_snapshot.get("latest_odds_timestamp"),
        "movement_10_to_now": None if not market_snapshot else market_snapshot.get("movement_10_to_now"),
        "movement_5_to_now": None if not market_snapshot else market_snapshot.get("movement_5_to_now"),
        "movement_3_to_now": None if not market_snapshot else market_snapshot.get("movement_3_to_now"),
        "movement_1_to_now": None if not market_snapshot else market_snapshot.get("movement_1_to_now"),
        "movement_score": movement_score,
        "form_score": recent_form.get("form_score"),
        "edge": edge,
        "combined_score": combined_score,
        "market_rank": market_rank,
        "history_row_count": recent_form.get("history_row_count", 0),
        "scratching_flag": runner.scratching_flag,
        "reason": skip_reason,
    }


def _build_runner_signal(db, race, runner: Runner, market_snapshot: dict[str, object], market_rank: int | None):
    if runner.scratching_flag:
        return None, "scratched_runner"

    prediction = (
        db.query(Prediction)
        .filter(Prediction.race_id == runner.race_id, Prediction.runner_id == runner.id)
        .first()
    )
    if not prediction or prediction.model_probability is None:
        return None, "missing_prediction"
    if prediction.model_probability > MAX_MODEL_PROBABILITY:
        return None, "model_probability_cap"

    latest_odds = market_snapshot["latest_odds"]
    if latest_odds is None:
        return None, "missing_odds"

    recent_form = _build_recent_form(_recent_history_rows(db, runner.horse_name))
    if not recent_form["has_history"] or recent_form["history_row_count"] < 1:
        return None, "missing_form_history"
    if not recent_form["qualifies"]:
        return None, "poor_recent_form"
    if recent_form["form_score"] < MIN_FORM_SCORE:
        return None, "form_score_too_low"

    market_probability = commission_adjusted_market_probability(latest_odds, COMMISSION_RATE)
    if market_probability is None:
        return None, "invalid_market_probability"
    edge = calculate_edge(prediction.model_probability, market_probability)
    if edge is None:
        return None, "invalid_market_probability"
    if edge < MIN_EDGE:
        return None, "edge_too_negative"

    movement_metrics = _movement_metrics(market_snapshot)
    if not movement_metrics["has_movement"]:
        return None, movement_metrics["skip_reason"]
    if movement_metrics["recent_drift"]:
        return None, "recent_drift"

    movement_score = movement_metrics["movement_score"]
    minutes_to_jump = _minutes_to_jump(race)
    combined_score = _signal_scores(movement_score, edge, recent_form["form_score"])

    signal = {
        "runner": runner,
        "race": race,
        "latest_odds": latest_odds,
        "latest_odds_timestamp": market_snapshot.get("latest_odds_timestamp"),
        "market_probability": market_probability,
        "model_probability": prediction.model_probability,
        "edge": edge,
        "form_score": recent_form["form_score"],
        "market_rank": market_rank,
        "odds_10m": market_snapshot.get("odds_10m"),
        "odds_5m": market_snapshot.get("odds_5m"),
        "odds_3m": market_snapshot.get("odds_3m"),
        "odds_1m": market_snapshot.get("odds_1m"),
        "movement_10_to_now": market_snapshot.get("movement_10_to_now"),
        "movement_5_to_now": market_snapshot.get("movement_5_to_now"),
        "movement_3_to_now": market_snapshot.get("movement_3_to_now"),
        "movement_1_to_now": market_snapshot.get("movement_1_to_now"),
        "movement_score": movement_score,
        "minutes_to_jump": minutes_to_jump,
        "combined_score": combined_score,
        "qualification_reason": recent_form["qualification_reason"],
        "last_start_finish": recent_form["last_start_finish"],
        "avg_last3_finish": recent_form["avg_last3_finish"],
        "avg_last3_margin": recent_form["avg_last3_margin"],
        "history_row_count": recent_form["history_row_count"],
        "scratching_flag": runner.scratching_flag,
    }

    if market_rank is None or market_rank > MAX_MARKET_RANK:
        return None, "market_rank_too_low"

    if MIN_RUNNER_ODDS <= latest_odds <= MAX_RUNNER_ODDS:
        if movement_score < MIN_MOVEMENT_SCORE:
            return None, "movement_score_too_low"
        if combined_score < MIN_COMBINED_SCORE:
            return None, "combined_score_too_low"
        return signal, None

    if WATCHLIST_MIN_ODDS <= latest_odds <= WATCHLIST_MAX_ODDS and movement_score >= WATCHLIST_MOVEMENT_SCORE:
        signal["watchlist_only"] = True
        return signal, None

    return None, "odds_band"


def _create_paper_bet(db, signal: dict[str, object], stake: float):
    latest_reset = get_latest_reset(db, DECISION_VERSION)
    paper_bet = PaperBet(
        race_id=signal["race"].id,
        runner_id=signal["runner"].id,
        odds_taken=signal["latest_odds"],
        market_probability=signal["market_probability"],
        model_probability=signal["model_probability"],
        edge=signal["edge"],
        form_score=signal["form_score"],
        combined_score=signal["combined_score"],
        qualification_reason=f"late_market_v2:{signal['qualification_reason']}",
        last_start_finish=signal["last_start_finish"],
        avg_last3_finish=signal["avg_last3_finish"],
        avg_last3_margin=signal["avg_last3_margin"],
        stake=stake,
        commission_rate=COMMISSION_RATE,
        decision_reason=(
            f"movement_score={signal['movement_score']:.4f} | combined_score={signal['combined_score']:.4f} | "
            f"edge={signal['edge']:.4f} | form_score={signal['form_score']:.4f} | "
            f"market_rank={signal['market_rank']} | minutes_to_jump={signal['minutes_to_jump']} | "
            f"latest_odds_timestamp={_format_timestamp(signal['latest_odds_timestamp'])} | "
            f"odds_10m={signal['odds_10m']} | odds_5m={signal['odds_5m']} | "
            f"odds_3m={signal['odds_3m']} | odds_1m={signal['odds_1m']} | "
            f"movement_10_to_now={signal['movement_10_to_now']} | movement_5_to_now={signal['movement_5_to_now']} | "
            f"movement_3_to_now={signal['movement_3_to_now']} | movement_1_to_now={signal['movement_1_to_now']} | "
            f"scratching_flag={signal['scratching_flag']}"
        ),
        result=None,
        profit_loss=None,
        settled_flag=False,
        decision_version=DECISION_VERSION,
        paper_bank_reset_id=latest_reset.id if latest_reset else None,
        proposed_notified_at=None,
        settlement_notified_at=None,
    )
    db.add(paper_bet)
    return paper_bet


def _send_proposed_notification(db, bet: PaperBet) -> bool:
    bet_detail = enrich_paper_bets(db, [bet])[0]
    strategy_bank = get_strategy_bank(db, bet.decision_version or DECISION_VERSION)
    details = bet_detail.get("decision_details", {})
    message = (
        "PROPOSED LATE BET — model_edge_late_v2\n"
        f"Horse: {bet_detail['horse_name']}\n"
        f"Track/Race: {bet_detail['track'] or 'Unknown'} R{bet_detail['race_number'] or '?'}\n"
        f"Minutes to Jump: {details.get('minutes_to_jump', 'Unknown')}\n"
        f"Odds Taken: {bet_detail['odds_taken']:.2f}\n"
        f"Stake: ${bet_detail['stake']:.2f}\n"
        f"Movement Score: {details.get('movement_score', 'n/a')}\n"
        f"Move 10m->Now: {details.get('movement_10_to_now', 'n/a')}\n"
        f"Move 5m->Now: {details.get('movement_5_to_now', 'n/a')}\n"
        f"Move 3m->Now: {details.get('movement_3_to_now', 'n/a')}\n"
        f"Move 1m->Now: {details.get('movement_1_to_now', 'n/a')}\n"
        f"Form Score: {(bet_detail['form_score'] or 0.0):.4f}\n"
        f"Edge: {bet_detail['edge']:.4f}\n"
        f"Combined Score: {(bet_detail['combined_score'] or 0.0):.4f}\n"
        f"Market Rank: {details.get('market_rank', 'Unknown')}\n"
        f"Strategy Bank: ${strategy_bank:.2f}\n"
        "Scratching Flag: False"
    )
    if send_telegram_message(message):
        bet.proposed_notified_at = datetime.utcnow()
        return True
    return False


def _send_watchlist_notification(db, signal: dict[str, object]) -> bool:
    meeting = getattr(signal["race"], "meeting", None)
    strategy_bank = get_strategy_bank(db, DECISION_VERSION)
    message = (
        "WATCHLIST ONLY — model_edge_late_v2 — NO BET PLACED\n"
        f"Horse: {signal['runner'].horse_name}\n"
        f"Track/Race: {(meeting.track if meeting else 'Unknown')} R{signal['race'].race_number or '?'}\n"
        f"Minutes to Jump: {signal['minutes_to_jump']}\n"
        f"Odds Taken: {signal['latest_odds']:.2f}\n"
        f"Movement Score: {signal['movement_score']:.4f}\n"
        f"Move 10m->Now: {signal['movement_10_to_now']}\n"
        f"Move 5m->Now: {signal['movement_5_to_now']}\n"
        f"Move 3m->Now: {signal['movement_3_to_now']}\n"
        f"Move 1m->Now: {signal['movement_1_to_now']}\n"
        f"Form Score: {signal['form_score']:.4f}\n"
        f"Edge: {signal['edge']:.4f}\n"
        f"Combined Score: {signal['combined_score']:.4f}\n"
        f"Market Rank: {signal['market_rank']}\n"
        f"Strategy Bank: ${strategy_bank:.2f}"
    )
    return send_telegram_message(message)


def _notify_unsent_bets(db) -> int:
    bets = (
        db.query(PaperBet)
        .filter(PaperBet.decision_version == DECISION_VERSION)
        .filter(PaperBet.settled_flag == False)
        .filter(PaperBet.proposed_notified_at.is_(None))
        .order_by(PaperBet.id.asc())
        .all()
    )
    sent = 0
    for bet in bets:
        if _send_proposed_notification(db, bet):
            sent += 1
    return sent


def create_late_market_v2_bets():
    init_db()
    db = SessionLocal()
    try:
        races = db.query(Race).filter(Race.betfair_market_id.isnot(None)).all()
        remaining_daily_bets = max(0, MAX_DAILY_BETS - _daily_bet_count(db))

        counters = {
            "races_checked": 0,
            "races_in_time_window": 0,
            "candidates_found": 0,
            "bets_created": 0,
            "watchlist_only": 0,
            "races_skipped_no_jump_time": 0,
            "races_skipped_not_in_time_window": 0,
            "races_skipped_existing_bet": 0,
            "runners_skipped_missing_prediction": 0,
            "runners_skipped_model_probability_cap": 0,
            "runners_skipped_scratched_runner": 0,
            "runners_skipped_missing_odds": 0,
            "runners_skipped_odds_band": 0,
            "runners_skipped_missing_form_history": 0,
            "runners_skipped_poor_recent_form": 0,
            "runners_skipped_form_score_too_low": 0,
            "runners_skipped_invalid_market_probability": 0,
            "runners_skipped_market_rank_too_low": 0,
            "runners_skipped_missing_late_movement": 0,
            "runners_skipped_movement_score_too_low": 0,
            "runners_skipped_edge_too_negative": 0,
            "runners_skipped_combined_score_too_low": 0,
            "runners_skipped_recent_drift": 0,
            "runners_skipped_daily_bet_cap": 0,
        }

        race_best_candidates = []
        rejected_logs: list[dict[str, object]] = []
        watchlist_signals: list[dict[str, object]] = []

        for race in races:
            counters["races_checked"] += 1
            in_window, minutes = _in_late_window(race)
            if minutes is None:
                counters["races_skipped_no_jump_time"] += 1
                continue
            if not in_window:
                counters["races_skipped_not_in_time_window"] += 1
                continue
            counters["races_in_time_window"] += 1

            if _race_already_has_bet(db, race.id):
                counters["races_skipped_existing_bet"] += 1
                continue

            runners = db.query(Runner).filter(Runner.race_id == race.id).all()
            snapshot_by_runner = {}
            for runner in runners:
                snapshot = _runner_market_snapshot(db, runner)
                if snapshot is not None:
                    snapshot_by_runner[runner.id] = snapshot

            market_ranks = _race_market_ranks(snapshot_by_runner)
            race_candidates = []
            for runner in runners:
                market_snapshot = snapshot_by_runner.get(runner.id)
                if market_snapshot is None:
                    counters["runners_skipped_missing_odds"] += 1
                    continue
                signal, skip_reason = _build_runner_signal(db, race, runner, market_snapshot, market_ranks.get(runner.id))
                if signal is None:
                    counters[f"runners_skipped_{skip_reason}"] += 1
                    rejected_logs.append(
                        _build_rejection_log(db, race, runner, market_snapshot, market_ranks.get(runner.id), skip_reason, minutes)
                    )
                    continue
                if signal.get("watchlist_only"):
                    watchlist_signals.append(signal)
                    counters["watchlist_only"] += 1
                    continue
                race_candidates.append(signal)
                counters["candidates_found"] += 1

            if race_candidates:
                race_candidates.sort(key=lambda item: item["combined_score"], reverse=True)
                race_best_candidates.append(race_candidates[0])

        race_best_candidates.sort(key=lambda item: item["combined_score"], reverse=True)
        selected = race_best_candidates[:remaining_daily_bets]
        counters["runners_skipped_daily_bet_cap"] = max(0, len(race_best_candidates) - len(selected))

        for signal in selected:
            stake = get_strategy_next_stake(db, DECISION_VERSION)
            _create_paper_bet(db, signal, stake)
            counters["bets_created"] += 1

        db.commit()
        notifications_sent = _notify_unsent_bets(db)
        db.commit()

        watchlist_sent = 0
        for signal in watchlist_signals[:MAX_REJECTION_LOG_ROWS]:
            if _send_watchlist_notification(db, signal):
                watchlist_sent += 1

        print(f"STRATEGY BANK {DECISION_VERSION}: ${get_strategy_bank(db, DECISION_VERSION):.2f}")
        print(f"RACES IN TIME WINDOW: {counters['races_in_time_window']}")
        print(f"CANDIDATES FOUND: {counters['candidates_found']}")
        print(f"PAPER BETS CREATED: {counters['bets_created']}")
        print(f"WATCHLIST ONLY SIGNALS: {counters['watchlist_only']}")
        print(f"PROPOSED LATE BET NOTIFICATIONS SENT: {notifications_sent}")
        print(f"WATCHLIST NOTIFICATIONS SENT: {watchlist_sent}")
        print(f"RACES SKIPPED DUE TO NOT IN TIME WINDOW: {counters['races_skipped_not_in_time_window']}")
        print(f"RACES SKIPPED DUE TO ALREADY BET RACE: {counters['races_skipped_existing_bet']}")
        print(f"RUNNERS SKIPPED DUE TO SCRATCHED RUNNER: {counters['runners_skipped_scratched_runner']}")
        print(f"RUNNERS SKIPPED DUE TO MISSING FORM HISTORY: {counters['runners_skipped_missing_form_history']}")
        print(f"RUNNERS SKIPPED DUE TO FORM SCORE TOO LOW: {counters['runners_skipped_form_score_too_low']}")
        print(f"RUNNERS SKIPPED DUE TO ODDS BAND: {counters['runners_skipped_odds_band']}")
        print(f"RUNNERS SKIPPED DUE TO MARKET RANK TOO LOW: {counters['runners_skipped_market_rank_too_low']}")
        print(f"RUNNERS SKIPPED DUE TO MISSING LATE MOVEMENT: {counters['runners_skipped_missing_late_movement']}")
        print(f"RUNNERS SKIPPED DUE TO MOVEMENT SCORE TOO LOW: {counters['runners_skipped_movement_score_too_low']}")
        print(f"RUNNERS SKIPPED DUE TO EDGE TOO NEGATIVE: {counters['runners_skipped_edge_too_negative']}")
        print(f"RUNNERS SKIPPED DUE TO COMBINED SCORE TOO LOW: {counters['runners_skipped_combined_score_too_low']}")
        print(f"RUNNERS SKIPPED DUE TO RECENT DRIFT: {counters['runners_skipped_recent_drift']}")
        print(f"RUNNERS SKIPPED DUE TO DAILY CAP: {counters['runners_skipped_daily_bet_cap']}")

        if race_best_candidates:
            print("TOP LATE V2 CANDIDATES")
            for row in race_best_candidates[:MAX_REJECTION_LOG_ROWS]:
                meeting = getattr(row["race"], "meeting", None)
                print(
                    " | ".join(
                        [
                            f"horse={row['runner'].horse_name}",
                            f"race={(meeting.track if meeting else 'Unknown')} R{row['race'].race_number or '?'}",
                            f"minutes_to_jump={row['minutes_to_jump']}",
                            f"latest_odds={row['latest_odds']}",
                            f"odds_10m={row['odds_10m']}",
                            f"odds_5m={row['odds_5m']}",
                            f"odds_3m={row['odds_3m']}",
                            f"odds_1m={row['odds_1m']}",
                            f"movement_10_to_now={row['movement_10_to_now']}",
                            f"movement_5_to_now={row['movement_5_to_now']}",
                            f"movement_3_to_now={row['movement_3_to_now']}",
                            f"movement_1_to_now={row['movement_1_to_now']}",
                            f"movement_score={row['movement_score']}",
                            f"form_score={row['form_score']}",
                            f"edge={row['edge']}",
                            f"combined_score={row['combined_score']}",
                            f"market_rank={row['market_rank']}",
                            f"history_rows={row['history_row_count']}",
                            f"scratching_flag={row['scratching_flag']}",
                            "reason=candidate",
                        ]
                    )
                )

        if rejected_logs:
            print("TOP REJECTED LATE V2 RUNNERS")
            rejected_logs.sort(
                key=lambda row: (
                    0 if row["reason"] in {"recent_drift", "movement_score_too_low", "missing_late_movement"} else 1,
                    row["minutes_to_jump"] if row["minutes_to_jump"] is not None else 9999.0,
                    -(row["movement_score"] or 0.0),
                )
            )
            for row in rejected_logs[:MAX_REJECTION_LOG_ROWS]:
                print(
                    " | ".join(
                        [
                            f"horse={row['horse_name']}",
                            f"race={(row['track'] or 'Unknown')} R{row['race_number'] or '?'}",
                            f"minutes_to_jump={row['minutes_to_jump']}",
                            f"latest_odds={row['latest_odds']}",
                            f"odds_10m={row['odds_10m']}",
                            f"odds_5m={row['odds_5m']}",
                            f"odds_3m={row['odds_3m']}",
                            f"odds_1m={row['odds_1m']}",
                            f"movement_10_to_now={row['movement_10_to_now']}",
                            f"movement_5_to_now={row['movement_5_to_now']}",
                            f"movement_3_to_now={row['movement_3_to_now']}",
                            f"movement_1_to_now={row['movement_1_to_now']}",
                            f"movement_score={row['movement_score']}",
                            f"form_score={row['form_score']}",
                            f"edge={row['edge']}",
                            f"combined_score={row['combined_score']}",
                            f"market_rank={row['market_rank']}",
                            f"history_rows={row['history_row_count']}",
                            f"scratching_flag={row['scratching_flag']}",
                            f"reason={row['reason']}",
                        ]
                    )
                )
    finally:
        db.close()


if __name__ == "__main__":
    create_late_market_v2_bets()
