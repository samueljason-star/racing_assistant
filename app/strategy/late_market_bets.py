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
DECISION_VERSION = "model_edge_late_v1"
COMMISSION_RATE = BETFAIR_COMMISSION_RATE
MAX_MODEL_PROBABILITY = PAPER_MAX_MODEL_PROBABILITY
MIN_FORM_SCORE = 0.30
MIN_RUNNER_ODDS = 3.0
MAX_RUNNER_ODDS = 30.0
MINUTES_TO_JUMP_MIN = 6.0
MINUTES_TO_JUMP_MAX = 12.0
MAX_DAILY_BETS = 5
POSITIVE_MOVEMENT_MIN = 0.01
MIN_ALLOWED_BEST_MOVEMENT = -0.02
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


def _runner_market_snapshot(db, race, runner: Runner) -> dict[str, float | None] | None:
    jump_time = _parse_jump_time(race.jump_time)
    if not jump_time:
        return None

    current_time = _now_utc()
    snapshots = _get_runner_snapshots(db, runner.id)
    if not snapshots:
        return None

    usable = []
    for snapshot in snapshots:
        ts = snapshot.timestamp
        if ts is None:
            continue
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        if ts <= current_time:
            usable.append(snapshot)

    if not usable:
        return None

    latest = usable[-1]
    opening = usable[0]
    cut_60 = _snapshot_before(usable, jump_time - timedelta(minutes=60))
    cut_30 = _snapshot_before(usable, jump_time - timedelta(minutes=30))
    cut_10 = _snapshot_before(usable, jump_time - timedelta(minutes=10))

    latest_odds = _safe_odds(latest)
    if latest_odds is None:
        return None

    opening_odds = _safe_odds(opening)
    odds_60 = _safe_odds(cut_60)
    odds_30 = _safe_odds(cut_30)
    odds_10 = _safe_odds(cut_10)

    return {
        "latest_odds": latest_odds,
        "opening_odds": opening_odds,
        "odds_60m": odds_60,
        "odds_30m": odds_30,
        "odds_10m": odds_10,
        "open_to_current_movement": (opening_odds - latest_odds) if opening_odds is not None else None,
        "60_to_current_movement": (odds_60 - latest_odds) if odds_60 is not None else None,
        "30_to_current_movement": (odds_30 - latest_odds) if odds_30 is not None else None,
    }


def _race_market_ranks(snapshot_by_runner: dict[int, dict[str, float | None]]) -> dict[int, int]:
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


def _positive_late_signal(market_snapshot: dict[str, float | None]) -> tuple[bool, float]:
    movements = [
        value
        for value in (
            market_snapshot.get("open_to_current_movement"),
            market_snapshot.get("60_to_current_movement"),
            market_snapshot.get("30_to_current_movement"),
        )
        if value is not None
    ]
    if not movements:
        return False, 0.0
    best_movement = max(movements)
    return best_movement >= MIN_ALLOWED_BEST_MOVEMENT, round(best_movement, 4)


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


def _build_rejection_log(
    db,
    race,
    runner: Runner,
    market_snapshot: dict[str, float | None] | None,
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
    has_signal, best_movement = _positive_late_signal(market_snapshot or {})
    recent_form = _build_recent_form(_recent_history_rows(db, runner.horse_name))

    return {
        "track": meeting.track if meeting else None,
        "race_number": race.race_number,
        "horse_name": runner.horse_name,
        "minutes_to_jump": minutes_to_jump,
        "latest_odds": latest_odds,
        "form_score": recent_form.get("form_score"),
        "market_rank": market_rank,
        "open_to_current_movement": None if not market_snapshot else market_snapshot.get("open_to_current_movement"),
        "60_to_current_movement": None if not market_snapshot else market_snapshot.get("60_to_current_movement"),
        "30_to_current_movement": None if not market_snapshot else market_snapshot.get("30_to_current_movement"),
        "model_probability": model_probability,
        "edge": edge,
        "best_movement": best_movement if has_signal or best_movement else 0.0,
        "skip_reason": skip_reason,
        "history_row_count": recent_form.get("history_row_count", 0),
    }


def _build_runner_signal(db, race, runner: Runner, market_snapshot: dict[str, float | None], market_rank: int | None):
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
    if not (MIN_RUNNER_ODDS <= latest_odds <= MAX_RUNNER_ODDS):
        return None, "odds_band"

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
    if edge <= 0:
        return None, "non_positive_edge"

    available_movements = [
        value
        for value in (
            market_snapshot.get("open_to_current_movement"),
            market_snapshot.get("60_to_current_movement"),
            market_snapshot.get("30_to_current_movement"),
        )
        if value is not None
    ]
    if not available_movements:
        return None, "missing_late_movement"

    has_signal, best_movement = _positive_late_signal(market_snapshot)
    if not has_signal:
        return None, "late_movement_too_negative"

    minutes_to_jump = _minutes_to_jump(race)
    score = round((edge * 0.55) + (recent_form["form_score"] * 0.25) + (best_movement * 0.20), 4)

    return {
        "runner": runner,
        "race": race,
        "latest_odds": latest_odds,
        "market_probability": market_probability,
        "model_probability": prediction.model_probability,
        "edge": edge,
        "form_score": recent_form["form_score"],
        "market_rank": market_rank,
        "opening_odds": market_snapshot.get("opening_odds"),
        "odds_60m": market_snapshot.get("odds_60m"),
        "odds_30m": market_snapshot.get("odds_30m"),
        "odds_10m": market_snapshot.get("odds_10m"),
        "open_to_current_movement": market_snapshot.get("open_to_current_movement"),
        "60_to_current_movement": market_snapshot.get("60_to_current_movement"),
        "30_to_current_movement": market_snapshot.get("30_to_current_movement"),
        "best_movement": best_movement,
        "minutes_to_jump": minutes_to_jump,
        "score": score,
        "qualification_reason": recent_form["qualification_reason"],
        "last_start_finish": recent_form["last_start_finish"],
        "avg_last3_finish": recent_form["avg_last3_finish"],
        "avg_last3_margin": recent_form["avg_last3_margin"],
        "history_row_count": recent_form["history_row_count"],
    }, None


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
        combined_score=signal["score"],
        qualification_reason=f"late_market:{signal['qualification_reason']}",
        last_start_finish=signal["last_start_finish"],
        avg_last3_finish=signal["avg_last3_finish"],
        avg_last3_margin=signal["avg_last3_margin"],
        stake=stake,
        commission_rate=COMMISSION_RATE,
        decision_reason=(
            f"late_market_score={signal['score']:.4f} | edge={signal['edge']:.4f} | "
            f"form_score={signal['form_score']:.4f} | market_rank={signal['market_rank']} | "
            f"minutes_to_jump={signal['minutes_to_jump']} | "
            f"open_to_current={signal['open_to_current_movement']} | "
            f"60_to_current={signal['60_to_current_movement']} | "
            f"30_to_current={signal['30_to_current_movement']} | "
            f"opening_odds={signal['opening_odds']} | odds_60m={signal['odds_60m']} | "
            f"odds_30m={signal['odds_30m']} | odds_10m={signal['odds_10m']}"
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
    details = {}
    for part in (bet.decision_reason or "").split("|"):
        if "=" in part:
            key, value = part.split("=", 1)
            details[key.strip()] = value.strip()
    message = (
        "PROPOSED LATE BET — model_edge_late_v1\n"
        f"Horse: {bet_detail['horse_name']}\n"
        f"Track/Race: {bet_detail['track'] or 'Unknown'} R{bet_detail['race_number'] or '?'}\n"
        f"Race Time: {bet_detail['jump_time'] or 'Unknown'}\n"
        f"Minutes to Jump: {details.get('minutes_to_jump', 'Unknown')}\n"
        f"Odds: {bet_detail['odds_taken']:.2f}\n"
        f"Stake: ${bet_detail['stake']:.2f}\n"
        f"Form Score: {(bet_detail['form_score'] or 0.0):.4f}\n"
        f"Market Rank: {details.get('market_rank', 'Unknown')}\n"
        f"Open-to-Current: {details.get('open_to_current', 'n/a')}\n"
        f"60-to-Current: {details.get('60_to_current', 'n/a')}\n"
        f"Edge/Score: {bet_detail['edge']:.4f} / {(bet_detail['combined_score'] or 0.0):.4f}\n"
        f"Strategy Bank: ${strategy_bank:.2f}"
    )
    if send_telegram_message(message):
        bet.proposed_notified_at = datetime.utcnow()
        return True
    return False


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


def create_late_market_bets():
    init_db()
    db = SessionLocal()
    try:
        races = db.query(Race).filter(Race.betfair_market_id.isnot(None)).all()
        existing_daily_bets = _daily_bet_count(db)
        remaining_daily_bets = max(0, MAX_DAILY_BETS - existing_daily_bets)

        counters = {
            "races_checked": 0,
            "races_in_time_window": 0,
            "candidates_found": 0,
            "bets_created": 0,
            "races_skipped_no_jump_time": 0,
            "races_skipped_not_in_time_window": 0,
            "races_skipped_existing_bet": 0,
            "runners_skipped_missing_prediction": 0,
            "runners_skipped_model_probability_cap": 0,
            "runners_skipped_missing_odds": 0,
            "runners_skipped_odds_band": 0,
            "runners_skipped_missing_form_history": 0,
            "runners_skipped_poor_recent_form": 0,
            "runners_skipped_form_score_too_low": 0,
            "runners_skipped_invalid_market_probability": 0,
            "runners_skipped_non_positive_edge": 0,
            "runners_skipped_missing_late_movement": 0,
            "runners_skipped_late_movement_too_negative": 0,
            "runners_skipped_daily_bet_cap": 0,
        }

        race_best_candidates = []
        rejected_logs: list[dict[str, object]] = []

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
                snapshot = _runner_market_snapshot(db, race, runner)
                if snapshot is not None:
                    snapshot_by_runner[runner.id] = snapshot

            market_ranks = _race_market_ranks(snapshot_by_runner)
            race_candidates = []
            for runner in runners:
                market_snapshot = snapshot_by_runner.get(runner.id)
                if market_snapshot is None:
                    counters["runners_skipped_missing_odds"] += 1
                    continue
                signal, skip_reason = _build_runner_signal(
                    db,
                    race,
                    runner,
                    market_snapshot,
                    market_ranks.get(runner.id),
                )
                if signal is None:
                    counters[f"runners_skipped_{skip_reason}"] += 1
                    rejected_logs.append(
                        _build_rejection_log(
                            db,
                            race,
                            runner,
                            market_snapshot,
                            market_ranks.get(runner.id),
                            skip_reason,
                            minutes,
                        )
                    )
                    continue
                race_candidates.append(signal)
                counters["candidates_found"] += 1

            if race_candidates:
                race_candidates.sort(key=lambda item: item["score"], reverse=True)
                race_best_candidates.append(race_candidates[0])

        race_best_candidates.sort(key=lambda item: item["score"], reverse=True)
        selected = race_best_candidates[:remaining_daily_bets]
        counters["runners_skipped_daily_bet_cap"] = max(0, len(race_best_candidates) - len(selected))

        for signal in selected:
            stake = get_strategy_next_stake(db, DECISION_VERSION)
            _create_paper_bet(db, signal, stake)
            counters["bets_created"] += 1

        db.commit()
        notifications_sent = _notify_unsent_bets(db)
        db.commit()

        print(f"STRATEGY BANK {DECISION_VERSION}: ${get_strategy_bank(db, DECISION_VERSION):.2f}")
        print(f"RACES CHECKED: {counters['races_checked']}")
        print(f"RACES IN TIME WINDOW: {counters['races_in_time_window']}")
        print(f"CANDIDATES FOUND: {counters['candidates_found']}")
        print(f"PAPER BETS CREATED: {counters['bets_created']}")
        print(f"PROPOSED LATE BET NOTIFICATIONS SENT: {notifications_sent}")
        print(f"RACES SKIPPED DUE TO NO JUMP TIME: {counters['races_skipped_no_jump_time']}")
        print(f"RACES SKIPPED OUTSIDE TIME WINDOW: {counters['races_skipped_not_in_time_window']}")
        print(f"RACES SKIPPED DUE TO EXISTING LATE BET: {counters['races_skipped_existing_bet']}")
        print(f"RUNNERS SKIPPED DUE TO MISSING PREDICTION: {counters['runners_skipped_missing_prediction']}")
        print(f"RUNNERS SKIPPED DUE TO MODEL PROBABILITY CAP: {counters['runners_skipped_model_probability_cap']}")
        print(f"RUNNERS SKIPPED DUE TO MISSING ODDS: {counters['runners_skipped_missing_odds']}")
        print(f"RUNNERS SKIPPED DUE TO ODDS BAND: {counters['runners_skipped_odds_band']}")
        print(f"RUNNERS SKIPPED DUE TO MISSING FORM HISTORY: {counters['runners_skipped_missing_form_history']}")
        print(f"RUNNERS SKIPPED DUE TO POOR RECENT FORM: {counters['runners_skipped_poor_recent_form']}")
        print(f"RUNNERS SKIPPED DUE TO FORM SCORE TOO LOW: {counters['runners_skipped_form_score_too_low']}")
        print(f"RUNNERS SKIPPED DUE TO INVALID MARKET PROBABILITY: {counters['runners_skipped_invalid_market_probability']}")
        print(f"RUNNERS SKIPPED DUE TO NON-POSITIVE EDGE: {counters['runners_skipped_non_positive_edge']}")
        print(f"RUNNERS SKIPPED DUE TO MISSING LATE MOVEMENT: {counters['runners_skipped_missing_late_movement']}")
        print(f"RUNNERS SKIPPED DUE TO LATE MOVEMENT TOO NEGATIVE: {counters['runners_skipped_late_movement_too_negative']}")
        print(f"RUNNERS SKIPPED DUE TO DAILY BET CAP: {counters['runners_skipped_daily_bet_cap']}")
        if rejected_logs:
            print("TOP REJECTED LATE RUNNERS")
            rejected_logs.sort(
                key=lambda row: (
                    0 if row["skip_reason"] in {"late_movement_too_negative", "missing_late_movement"} else 1,
                    row["minutes_to_jump"] if row["minutes_to_jump"] is not None else 9999.0,
                    -(row["form_score"] or 0.0),
                )
            )
            for row in rejected_logs[:MAX_REJECTION_LOG_ROWS]:
                print(
                    " | ".join(
                        [
                            f"horse={row['horse_name']}",
                            f"race={(row['track'] or 'Unknown')} R{row['race_number'] or '?'}",
                            f"minutes_to_jump={row['minutes_to_jump']}",
                            f"odds={row['latest_odds']}",
                            f"form_score={row['form_score']}",
                            f"market_rank={row['market_rank']}",
                            f"open_to_current={row['open_to_current_movement']}",
                            f"60_to_current={row['60_to_current_movement']}",
                            f"30_to_current={row['30_to_current_movement']}",
                            f"edge={row['edge']}",
                            f"best_movement={row['best_movement']}",
                            f"history_rows={row['history_row_count']}",
                            f"reason={row['skip_reason']}",
                        ]
                    )
                )
    finally:
        db.close()


if __name__ == "__main__":
    create_late_market_bets()
