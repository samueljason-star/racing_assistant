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
WATCHLIST_MIN_ODDS = 3.0
WATCHLIST_MAX_ODDS = 50.0
MAX_MARKET_RANK = 8
MIN_EDGE = -0.01
MIN_MOVEMENT_SCORE = 0.45
MIN_COMBINED_SCORE = 0.45
WATCHLIST_MOVEMENT_SCORE = 0.60
MAX_DAILY_BETS = 3
MAX_LOG_ROWS = 5
UPCOMING_RACE_LOG_ROWS = 10

WATCHING_STAGE = "WATCHING"
SHORTLIST_STAGE = "SHORTLIST"
FINAL_CHECK_STAGE = "FINAL_CHECK"


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _minutes_to_jump(race) -> float | None:
    jump_time = _parse_jump_time(race.jump_time)
    if not jump_time:
        return None
    return round((jump_time - _now_utc()).total_seconds() / 60.0, 2)


def _race_stage(minutes_to_jump: float | None) -> str | None:
    if minutes_to_jump is None:
        return None
    if 30.0 <= minutes_to_jump <= 60.0:
        return WATCHING_STAGE
    if 10.0 <= minutes_to_jump < 30.0:
        return SHORTLIST_STAGE
    if 1.0 <= minutes_to_jump < 10.0:
        return FINAL_CHECK_STAGE
    return None


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


def _safe_timestamp(snapshot: OddsSnapshot | None) -> datetime | None:
    if not snapshot or snapshot.timestamp is None:
        return None
    ts = snapshot.timestamp
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts


def _safe_odds(snapshot: OddsSnapshot | None) -> float | None:
    if not snapshot or snapshot.odds is None or snapshot.odds <= 0:
        return None
    return float(snapshot.odds)


def _format_timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(BRISBANE_TZ).strftime("%H:%M:%S")


def _snapshot_before(snapshots: list[OddsSnapshot], cutoff: datetime, evaluation_time: datetime) -> OddsSnapshot | None:
    if cutoff > evaluation_time:
        return None
    chosen = None
    for snapshot in snapshots:
        ts = _safe_timestamp(snapshot)
        if ts is None:
            continue
        if ts <= cutoff:
            chosen = snapshot
        else:
            break
    return chosen


def _runner_market_snapshot(db, race, runner: Runner) -> dict[str, object] | None:
    jump_time = _parse_jump_time(race.jump_time)
    if not jump_time:
        return None

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
    opening = usable[0]
    cut_60 = _snapshot_before(usable, jump_time - timedelta(minutes=60), evaluation_time)
    cut_30 = _snapshot_before(usable, jump_time - timedelta(minutes=30), evaluation_time)
    cut_10 = _snapshot_before(usable, jump_time - timedelta(minutes=10), evaluation_time)
    cut_5 = _snapshot_before(usable, jump_time - timedelta(minutes=5), evaluation_time)
    cut_3 = _snapshot_before(usable, jump_time - timedelta(minutes=3), evaluation_time)

    latest_odds = _safe_odds(latest)
    if latest_odds is None:
        return None

    opening_odds = _safe_odds(opening)
    odds_60 = _safe_odds(cut_60)
    odds_30 = _safe_odds(cut_30)
    odds_10 = _safe_odds(cut_10)
    odds_5 = _safe_odds(cut_5)
    odds_3 = _safe_odds(cut_3)

    return {
        "latest_odds": latest_odds,
        "latest_odds_timestamp": _safe_timestamp(latest),
        "opening_odds": opening_odds,
        "odds_60m": odds_60,
        "odds_30m": odds_30,
        "odds_10m": odds_10,
        "odds_5m": odds_5,
        "odds_3m": odds_3,
        "open_to_current": (opening_odds - latest_odds) if opening_odds is not None else None,
        "60_to_current": (odds_60 - latest_odds) if odds_60 is not None else None,
        "30_to_current": (odds_30 - latest_odds) if odds_30 is not None else None,
        "10_to_current": (odds_10 - latest_odds) if odds_10 is not None else None,
        "5_to_current": (odds_5 - latest_odds) if odds_5 is not None else None,
        "3_to_current": (odds_3 - latest_odds) if odds_3 is not None else None,
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


def _movement_metrics(market_snapshot: dict[str, object], market_rank: int | None) -> dict[str, object]:
    movements = {
        "open_to_current": market_snapshot.get("open_to_current"),
        "60_to_current": market_snapshot.get("60_to_current"),
        "30_to_current": market_snapshot.get("30_to_current"),
        "10_to_current": market_snapshot.get("10_to_current"),
        "5_to_current": market_snapshot.get("5_to_current"),
        "3_to_current": market_snapshot.get("3_to_current"),
    }
    available = [value for value in movements.values() if value is not None]
    if not available:
        return {
            "has_movement": False,
            "movement_score": 0.0,
            "recent_drift": False,
            "best_movement": None,
            "skip_reason": "missing_late_movement",
        }

    recent_drift = (
        (movements["3_to_current"] is not None and movements["3_to_current"] < 0)
        or (
            movements["5_to_current"] is not None
            and movements["5_to_current"] < 0
            and (movements["3_to_current"] is None or movements["3_to_current"] <= 0)
        )
        or (
            (movements["60_to_current"] or 0) > 0
            and (movements["30_to_current"] or 0) > 0
            and (movements["10_to_current"] is None or movements["10_to_current"] <= 0)
            and (movements["5_to_current"] is None or movements["5_to_current"] <= 0)
        )
    )

    weighted_score = 0.0
    total_weight = 0.0
    for key, weight in (
        ("open_to_current", 0.10),
        ("60_to_current", 0.15),
        ("30_to_current", 0.15),
        ("10_to_current", 0.20),
        ("5_to_current", 0.20),
        ("3_to_current", 0.20),
    ):
        movement = movements[key]
        if movement is None:
            continue
        total_weight += weight
        if movement > 0:
            component = min(movement / 2.5, 1.0)
        elif movement == 0:
            component = 0.22
        else:
            component = max(0.0, 0.18 + (movement / 2.5))
        weighted_score += component * weight

    movement_score = weighted_score / total_weight if total_weight else 0.0
    positive_count = sum(1 for value in available if value > 0)
    recent_positive = sum(
        1
        for value in (movements["10_to_current"], movements["5_to_current"], movements["3_to_current"])
        if value is not None and value > 0
    )
    if positive_count >= 4:
        movement_score += 0.10
    elif positive_count >= 3:
        movement_score += 0.06
    elif positive_count >= 2:
        movement_score += 0.03
    if recent_positive >= 2:
        movement_score += 0.08
    elif recent_positive == 1:
        movement_score += 0.03
    if market_rank is not None and market_rank <= 3:
        movement_score += 0.05
    elif market_rank is not None and market_rank <= 5:
        movement_score += 0.02
    if len(available) < 3:
        movement_score -= 0.10
    if recent_drift:
        movement_score -= 0.22

    movement_score = round(max(0.0, min(1.0, movement_score)), 4)
    return {
        "has_movement": True,
        "movement_score": movement_score,
        "recent_drift": recent_drift,
        "best_movement": round(max(available), 4),
        "skip_reason": "recent_drift" if recent_drift else None,
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


def _combined_score(movement_score: float, edge: float | None, form_score: float) -> float:
    edge_component = max(edge if edge is not None else -0.05, -0.05)
    return round((movement_score * 0.55) + (form_score * 0.30) + (edge_component * 0.15), 4)


def _build_base_signal(db, race, runner: Runner, market_snapshot: dict[str, object], market_rank: int | None, stage: str):
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

    latest_odds = market_snapshot.get("latest_odds")
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

    movement_metrics = _movement_metrics(market_snapshot, market_rank)
    if not movement_metrics["has_movement"]:
        return None, movement_metrics["skip_reason"]

    movement_score = movement_metrics["movement_score"]
    combined_score = _combined_score(movement_score, edge, recent_form["form_score"])

    return {
        "runner": runner,
        "race": race,
        "stage": stage,
        "minutes_to_jump": _minutes_to_jump(race),
        "latest_odds": latest_odds,
        "latest_odds_timestamp": market_snapshot.get("latest_odds_timestamp"),
        "opening_odds": market_snapshot.get("opening_odds"),
        "odds_60m": market_snapshot.get("odds_60m"),
        "odds_30m": market_snapshot.get("odds_30m"),
        "odds_10m": market_snapshot.get("odds_10m"),
        "odds_5m": market_snapshot.get("odds_5m"),
        "odds_3m": market_snapshot.get("odds_3m"),
        "open_to_current": market_snapshot.get("open_to_current"),
        "60_to_current": market_snapshot.get("60_to_current"),
        "30_to_current": market_snapshot.get("30_to_current"),
        "10_to_current": market_snapshot.get("10_to_current"),
        "5_to_current": market_snapshot.get("5_to_current"),
        "3_to_current": market_snapshot.get("3_to_current"),
        "market_probability": market_probability,
        "model_probability": prediction.model_probability,
        "edge": edge,
        "form_score": recent_form["form_score"],
        "movement_score": movement_score,
        "combined_score": combined_score,
        "market_rank": market_rank,
        "history_rows": recent_form["history_row_count"],
        "scratching_flag": runner.scratching_flag,
        "qualification_reason": recent_form["qualification_reason"],
        "last_start_finish": recent_form["last_start_finish"],
        "avg_last3_finish": recent_form["avg_last3_finish"],
        "avg_last3_margin": recent_form["avg_last3_margin"],
        "recent_drift": movement_metrics["recent_drift"],
    }, None


def _build_rejection_log(
    db,
    race,
    runner: Runner,
    market_snapshot: dict[str, object] | None,
    market_rank: int | None,
    stage: str | None,
    reason: str,
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
    movement_metrics = _movement_metrics(market_snapshot or {}, market_rank)
    movement_score = movement_metrics["movement_score"]
    form_score = recent_form.get("form_score") or 0.0
    combined_score = _combined_score(movement_score, edge, form_score)
    return {
        "stage": stage,
        "horse": runner.horse_name,
        "track": meeting.track if meeting else None,
        "race_number": race.race_number,
        "minutes_to_jump": minutes_to_jump,
        "latest_odds": latest_odds,
        "opening_odds": None if not market_snapshot else market_snapshot.get("opening_odds"),
        "odds_60m": None if not market_snapshot else market_snapshot.get("odds_60m"),
        "odds_30m": None if not market_snapshot else market_snapshot.get("odds_30m"),
        "odds_10m": None if not market_snapshot else market_snapshot.get("odds_10m"),
        "odds_5m": None if not market_snapshot else market_snapshot.get("odds_5m"),
        "odds_3m": None if not market_snapshot else market_snapshot.get("odds_3m"),
        "latest_odds_timestamp": None if not market_snapshot else market_snapshot.get("latest_odds_timestamp"),
        "open_to_current": None if not market_snapshot else market_snapshot.get("open_to_current"),
        "60_to_current": None if not market_snapshot else market_snapshot.get("60_to_current"),
        "30_to_current": None if not market_snapshot else market_snapshot.get("30_to_current"),
        "10_to_current": None if not market_snapshot else market_snapshot.get("10_to_current"),
        "5_to_current": None if not market_snapshot else market_snapshot.get("5_to_current"),
        "3_to_current": None if not market_snapshot else market_snapshot.get("3_to_current"),
        "movement_score": movement_score,
        "form_score": recent_form.get("form_score"),
        "edge": edge,
        "combined_score": combined_score,
        "market_rank": market_rank,
        "history_rows": recent_form.get("history_row_count", 0),
        "scratching_flag": runner.scratching_flag,
        "reason": reason,
    }


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
            f"stage={signal['stage']} | movement_score={signal['movement_score']:.4f} | "
            f"combined_score={signal['combined_score']:.4f} | edge={signal['edge']:.4f} | "
            f"form_score={signal['form_score']:.4f} | market_rank={signal['market_rank']} | "
            f"minutes_to_jump={signal['minutes_to_jump']} | latest_odds_timestamp={_format_timestamp(signal['latest_odds_timestamp'])} | "
            f"opening_odds={signal['opening_odds']} | odds_60m={signal['odds_60m']} | odds_30m={signal['odds_30m']} | "
            f"odds_10m={signal['odds_10m']} | odds_5m={signal['odds_5m']} | odds_3m={signal['odds_3m']} | "
            f"open_to_current={signal['open_to_current']} | 60_to_current={signal['60_to_current']} | "
            f"30_to_current={signal['30_to_current']} | 10_to_current={signal['10_to_current']} | "
            f"5_to_current={signal['5_to_current']} | 3_to_current={signal['3_to_current']} | "
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
        f"Move 10m->Now: {details.get('10_to_current', 'n/a')}\n"
        f"Move 5m->Now: {details.get('5_to_current', 'n/a')}\n"
        f"Move 3m->Now: {details.get('3_to_current', 'n/a')}\n"
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
    message = (
        "WATCHLIST ONLY — model_edge_late_v2 — NO BET PLACED\n"
        f"Horse: {signal['runner'].horse_name}\n"
        f"Track/Race: {(meeting.track if meeting else 'Unknown')} R{signal['race'].race_number or '?'}\n"
        f"Minutes to Jump: {signal['minutes_to_jump']}\n"
        f"Odds Taken: {signal['latest_odds']:.2f}\n"
        f"Movement Score: {signal['movement_score']:.4f}\n"
        f"Move 10m->Now: {signal['10_to_current']}\n"
        f"Move 5m->Now: {signal['5_to_current']}\n"
        f"Move 3m->Now: {signal['3_to_current']}\n"
        f"Form Score: {signal['form_score']:.4f}\n"
        f"Edge: {signal['edge']:.4f}\n"
        f"Combined Score: {signal['combined_score']:.4f}\n"
        f"Market Rank: {signal['market_rank']}"
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


def _print_upcoming_race_table(race_stage_rows: list[dict[str, object]]) -> None:
    print("UPCOMING LATE V2 RACES")
    for row in sorted(race_stage_rows, key=lambda item: item["minutes_to_jump"])[:UPCOMING_RACE_LOG_ROWS]:
        print(
            f"stage={row['stage']} | track={row['track']} | race_number={row['race_number']} | "
            f"minutes_to_jump={row['minutes_to_jump']}"
        )


def _print_signal_row(prefix: str, row: dict[str, object]) -> None:
    print(
        " | ".join(
            [
                prefix,
                f"stage={row['stage']}",
                f"horse={row.get('horse') or row['runner'].horse_name}",
                f"race={(row['track'] if 'track' in row else getattr(getattr(row['race'], 'meeting', None), 'track', None)) or 'Unknown'} "
                f"R{row.get('race_number') or row['race'].race_number or '?'}",
                f"minutes_to_jump={row['minutes_to_jump']}",
                f"latest_odds={row['latest_odds']}",
                f"opening_odds={row['opening_odds']}",
                f"odds_60m={row['odds_60m']}",
                f"odds_30m={row['odds_30m']}",
                f"odds_10m={row['odds_10m']}",
                f"odds_5m={row['odds_5m']}",
                f"odds_3m={row['odds_3m']}",
                f"open_to_current={row['open_to_current']}",
                f"60_to_current={row['60_to_current']}",
                f"30_to_current={row['30_to_current']}",
                f"10_to_current={row['10_to_current']}",
                f"5_to_current={row['5_to_current']}",
                f"3_to_current={row['3_to_current']}",
                f"movement_score={row['movement_score']}",
                f"form_score={row['form_score']}",
                f"edge={row['edge']}",
                f"combined_score={row['combined_score']}",
                f"market_rank={row['market_rank']}",
                f"reason={row['reason'] if 'reason' in row else 'candidate'}",
            ]
        )
    )


def create_late_market_v2_bets():
    init_db()
    db = SessionLocal()
    try:
        races = db.query(Race).filter(Race.betfair_market_id.isnot(None)).all()
        remaining_daily_bets = max(0, MAX_DAILY_BETS - _daily_bet_count(db))

        counters = {
            "races_in_watching_stage": 0,
            "races_in_shortlist_stage": 0,
            "races_in_final_check_stage": 0,
            "candidates_found": 0,
            "bets_created": 0,
            "watchlist_signals": 0,
            "not_in_time_window": 0,
            "already_bet_race": 0,
            "scratched_runner": 0,
            "missing_form_history": 0,
            "form_score_too_low": 0,
            "odds_band": 0,
            "market_rank_too_low": 0,
            "missing_late_movement": 0,
            "movement_score_too_low": 0,
            "recent_drift": 0,
            "edge_too_negative": 0,
            "combined_score_too_low": 0,
            "daily_cap": 0,
        }

        race_stage_rows: list[dict[str, object]] = []
        candidate_logs: list[dict[str, object]] = []
        rejected_logs: list[dict[str, object]] = []
        race_best_candidates: list[dict[str, object]] = []
        shortlist_watchlist_signals: list[dict[str, object]] = []

        for race in races:
            minutes_to_jump = _minutes_to_jump(race)
            stage = _race_stage(minutes_to_jump)
            meeting = getattr(race, "meeting", None)

            if stage is None:
                counters["not_in_time_window"] += 1
                continue

            race_stage_rows.append(
                {
                    "stage": stage,
                    "track": meeting.track if meeting else None,
                    "race_number": race.race_number,
                    "minutes_to_jump": minutes_to_jump,
                }
            )

            if stage == WATCHING_STAGE:
                counters["races_in_watching_stage"] += 1
            elif stage == SHORTLIST_STAGE:
                counters["races_in_shortlist_stage"] += 1
            elif stage == FINAL_CHECK_STAGE:
                counters["races_in_final_check_stage"] += 1

            if stage == FINAL_CHECK_STAGE and _race_already_has_bet(db, race.id):
                counters["already_bet_race"] += 1
                continue

            runners = db.query(Runner).filter(Runner.race_id == race.id).all()
            snapshot_by_runner = {}
            for runner in runners:
                snapshot = _runner_market_snapshot(db, race, runner)
                if snapshot is not None:
                    snapshot_by_runner[runner.id] = snapshot

            market_ranks = _race_market_ranks(snapshot_by_runner)
            race_final_candidates = []

            for runner in runners:
                market_snapshot = snapshot_by_runner.get(runner.id)
                if market_snapshot is None:
                    continue

                signal, base_skip_reason = _build_base_signal(db, race, runner, market_snapshot, market_ranks.get(runner.id), stage)
                if signal is None:
                    if base_skip_reason in counters:
                        counters[base_skip_reason] += 1
                    rejected_logs.append(
                        _build_rejection_log(db, race, runner, market_snapshot, market_ranks.get(runner.id), stage, base_skip_reason, minutes_to_jump)
                    )
                    continue

                if stage == WATCHING_STAGE:
                    candidate_logs.append(signal)
                    continue

                if stage == SHORTLIST_STAGE:
                    candidate_logs.append(signal)
                    if (
                        WATCHLIST_MIN_ODDS <= signal["latest_odds"] <= WATCHLIST_MAX_ODDS
                        and signal["movement_score"] >= WATCHLIST_MOVEMENT_SCORE
                        and signal["form_score"] >= MIN_FORM_SCORE
                    ):
                        shortlist_watchlist_signals.append(signal)
                        counters["watchlist_signals"] += 1
                    continue

                if signal["market_rank"] is None or signal["market_rank"] > MAX_MARKET_RANK:
                    counters["market_rank_too_low"] += 1
                    rejected_logs.append(
                        _build_rejection_log(db, race, runner, market_snapshot, market_ranks.get(runner.id), stage, "market_rank_too_low", minutes_to_jump)
                    )
                    continue

                if not (MIN_RUNNER_ODDS <= signal["latest_odds"] <= MAX_RUNNER_ODDS):
                    if WATCHLIST_MIN_ODDS <= signal["latest_odds"] <= WATCHLIST_MAX_ODDS and signal["movement_score"] >= WATCHLIST_MOVEMENT_SCORE:
                        shortlist_watchlist_signals.append(signal)
                        counters["watchlist_signals"] += 1
                        continue
                    counters["odds_band"] += 1
                    rejected_logs.append(
                        _build_rejection_log(db, race, runner, market_snapshot, market_ranks.get(runner.id), stage, "odds_band", minutes_to_jump)
                    )
                    continue

                if signal["recent_drift"]:
                    counters["recent_drift"] += 1
                    rejected_logs.append(
                        _build_rejection_log(db, race, runner, market_snapshot, market_ranks.get(runner.id), stage, "recent_drift", minutes_to_jump)
                    )
                    continue
                if signal["movement_score"] < MIN_MOVEMENT_SCORE:
                    counters["movement_score_too_low"] += 1
                    rejected_logs.append(
                        _build_rejection_log(db, race, runner, market_snapshot, market_ranks.get(runner.id), stage, "movement_score_too_low", minutes_to_jump)
                    )
                    continue
                if signal["combined_score"] < MIN_COMBINED_SCORE:
                    counters["combined_score_too_low"] += 1
                    rejected_logs.append(
                        _build_rejection_log(db, race, runner, market_snapshot, market_ranks.get(runner.id), stage, "combined_score_too_low", minutes_to_jump)
                    )
                    continue

                race_final_candidates.append(signal)
                candidate_logs.append(signal)
                counters["candidates_found"] += 1

            if race_final_candidates:
                race_final_candidates.sort(key=lambda item: item["combined_score"], reverse=True)
                race_best_candidates.append(race_final_candidates[0])

        race_best_candidates.sort(key=lambda item: item["combined_score"], reverse=True)
        selected = race_best_candidates[:remaining_daily_bets]
        counters["daily_cap"] = max(0, len(race_best_candidates) - len(selected))

        for signal in selected:
            stake = get_strategy_next_stake(db, DECISION_VERSION)
            _create_paper_bet(db, signal, stake)
            counters["bets_created"] += 1

        db.commit()
        notifications_sent = _notify_unsent_bets(db)
        db.commit()

        watchlist_notifications_sent = 0
        for signal in shortlist_watchlist_signals[:MAX_LOG_ROWS]:
            if _send_watchlist_notification(db, signal):
                watchlist_notifications_sent += 1

        _print_upcoming_race_table(race_stage_rows)
        print(f"RACES IN WATCHING STAGE: {counters['races_in_watching_stage']}")
        print(f"RACES IN SHORTLIST STAGE: {counters['races_in_shortlist_stage']}")
        print(f"RACES IN FINAL_CHECK STAGE: {counters['races_in_final_check_stage']}")
        print(f"CANDIDATES FOUND: {counters['candidates_found']}")
        print(f"PAPER BETS CREATED: {counters['bets_created']}")
        print(f"WATCHLIST SIGNALS: {counters['watchlist_signals']}")
        print(f"PROPOSED LATE BET NOTIFICATIONS SENT: {notifications_sent}")
        print(f"WATCHLIST NOTIFICATIONS SENT: {watchlist_notifications_sent}")
        print(f"RACES SKIPPED DUE TO NOT IN TIME WINDOW: {counters['not_in_time_window']}")
        print(f"RACES SKIPPED DUE TO ALREADY BET RACE: {counters['already_bet_race']}")
        print(f"RUNNERS SKIPPED DUE TO SCRATCHED RUNNER: {counters['scratched_runner']}")
        print(f"RUNNERS SKIPPED DUE TO MISSING FORM HISTORY: {counters['missing_form_history']}")
        print(f"RUNNERS SKIPPED DUE TO FORM SCORE TOO LOW: {counters['form_score_too_low']}")
        print(f"RUNNERS SKIPPED DUE TO ODDS BAND: {counters['odds_band']}")
        print(f"RUNNERS SKIPPED DUE TO MARKET RANK TOO LOW: {counters['market_rank_too_low']}")
        print(f"RUNNERS SKIPPED DUE TO MISSING LATE MOVEMENT: {counters['missing_late_movement']}")
        print(f"RUNNERS SKIPPED DUE TO MOVEMENT SCORE TOO LOW: {counters['movement_score_too_low']}")
        print(f"RUNNERS SKIPPED DUE TO RECENT DRIFT: {counters['recent_drift']}")
        print(f"RUNNERS SKIPPED DUE TO EDGE TOO NEGATIVE: {counters['edge_too_negative']}")
        print(f"RUNNERS SKIPPED DUE TO COMBINED SCORE TOO LOW: {counters['combined_score_too_low']}")
        print(f"RUNNERS SKIPPED DUE TO DAILY CAP: {counters['daily_cap']}")
        print(f"STRATEGY BANK {DECISION_VERSION}: ${get_strategy_bank(db, DECISION_VERSION):.2f}")

        if candidate_logs:
            print("TOP LATE V2 CANDIDATES")
            candidate_logs.sort(
                key=lambda row: (
                    0 if row["stage"] == FINAL_CHECK_STAGE else 1 if row["stage"] == SHORTLIST_STAGE else 2,
                    -(row["combined_score"]),
                )
            )
            for row in candidate_logs[:MAX_LOG_ROWS]:
                _print_signal_row("candidate", row)

        if rejected_logs:
            print("TOP REJECTED LATE V2 RUNNERS")
            rejected_logs.sort(
                key=lambda row: (
                    0 if row["stage"] == FINAL_CHECK_STAGE else 1,
                    row["minutes_to_jump"] if row["minutes_to_jump"] is not None else 9999.0,
                    -(row["movement_score"] or 0.0),
                )
            )
            for row in rejected_logs[:MAX_LOG_ROWS]:
                _print_signal_row("rejected", row)
    finally:
        db.close()


if __name__ == "__main__":
    create_late_market_v2_bets()
