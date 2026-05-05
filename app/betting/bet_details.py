from app.betting.market_helpers import edge_bucket_label, odds_bucket_label, raw_market_probability
from app.models import Meeting, Race, Runner


def _parse_decision_details(decision_reason):
    details = {}
    if not decision_reason:
        return details
    for part in decision_reason.split("|"):
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        details[key.strip()] = value.strip()
    return details


def get_runner_name_map(db, runner_ids):
    """Return a mapping of runner_id to horse name for the supplied ids."""
    if not runner_ids:
        return {}

    runners = db.query(Runner).filter(Runner.id.in_(sorted(set(runner_ids)))).all()
    return {runner.id: runner.horse_name for runner in runners}


def get_race_context_map(db, race_ids):
    """Return race context keyed by race_id including track and race metadata."""
    if not race_ids:
        return {}

    races = db.query(Race).filter(Race.id.in_(sorted(set(race_ids)))).all()
    meeting_ids = [race.meeting_id for race in races if race.meeting_id is not None]
    meetings = db.query(Meeting).filter(Meeting.id.in_(sorted(set(meeting_ids)))).all()
    meeting_map = {meeting.id: meeting for meeting in meetings}

    context = {}
    for race in races:
        meeting = meeting_map.get(race.meeting_id)
        context[race.id] = {
            "track": meeting.track if meeting else None,
            "race_number": race.race_number,
            "meeting_type": meeting.meeting_type if meeting else None,
            "race_type": race.class_name,
            "jump_time": race.jump_time,
        }
    return context


def enrich_paper_bets(db, bets):
    """Attach display-friendly fields such as horse_name to paper bets."""
    runner_name_map = get_runner_name_map(db, [bet.runner_id for bet in bets])
    race_context_map = get_race_context_map(db, [bet.race_id for bet in bets])
    enriched = []

    for bet in bets:
        race_context = race_context_map.get(bet.race_id, {})
        decision_details = _parse_decision_details(bet.decision_reason)
        enriched.append(
            {
                "id": bet.id,
                "horse_name": runner_name_map.get(bet.runner_id, f"Runner {bet.runner_id}"),
                "race_id": bet.race_id,
                "track": race_context.get("track"),
                "race_number": race_context.get("race_number"),
                "meeting_type": race_context.get("meeting_type"),
                "race_type": race_context.get("race_type"),
                "jump_time": race_context.get("jump_time"),
                "runner_id": bet.runner_id,
                "odds_taken": bet.odds_taken,
                "stake": bet.stake,
                "raw_market_probability": raw_market_probability(bet.odds_taken),
                "market_probability": bet.market_probability,
                "model_probability": bet.model_probability,
                "edge": bet.edge,
                "form_score": getattr(bet, "form_score", None),
                "combined_score": getattr(bet, "combined_score", None),
                "qualification_reason": getattr(bet, "qualification_reason", None),
                "last_start_finish": getattr(bet, "last_start_finish", None),
                "avg_last3_finish": getattr(bet, "avg_last3_finish", None),
                "avg_last3_margin": getattr(bet, "avg_last3_margin", None),
                "commission_rate": getattr(bet, "commission_rate", None),
                "decision_reason": bet.decision_reason,
                "decision_details": decision_details,
                "movement_score": decision_details.get("movement_score"),
                "movement_10_to_now": decision_details.get("movement_10_to_now"),
                "movement_5_to_now": decision_details.get("movement_5_to_now"),
                "movement_3_to_now": decision_details.get("movement_3_to_now"),
                "movement_1_to_now": decision_details.get("movement_1_to_now"),
                "latest_odds_timestamp": decision_details.get("latest_odds_timestamp"),
                "decision_version": bet.decision_version,
                "result": bet.result,
                "profit_loss": bet.profit_loss,
                "settled_flag": bet.settled_flag,
                "placed_at": getattr(bet, "placed_at", None),
                "settled_at": getattr(bet, "settled_at", None),
                "paper_bank_reset_id": getattr(bet, "paper_bank_reset_id", None),
                "closing_odds": getattr(bet, "closing_odds", None),
                "final_observed_odds": getattr(bet, "final_observed_odds", None),
                "closing_line_difference": getattr(bet, "closing_line_difference", None),
                "closing_line_pct": getattr(bet, "closing_line_pct", None),
                "clv_percent": getattr(bet, "clv_percent", getattr(bet, "closing_line_pct", None)),
                "beat_closing_line": getattr(bet, "beat_closing_line", None),
                "proposed_notified_at": getattr(bet, "proposed_notified_at", None),
                "settlement_notified_at": getattr(bet, "settlement_notified_at", None),
                "odds_bucket": odds_bucket_label(bet.odds_taken),
                "edge_bucket": edge_bucket_label(bet.edge),
            }
        )

    return enriched
