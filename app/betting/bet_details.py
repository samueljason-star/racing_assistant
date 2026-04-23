from app.models import Meeting, Race, Runner


def get_runner_name_map(db, runner_ids):
    """Return a mapping of runner_id to horse name for the supplied ids."""
    if not runner_ids:
        return {}

    runners = db.query(Runner).filter(Runner.id.in_(sorted(set(runner_ids)))).all()
    return {runner.id: runner.horse_name for runner in runners}


def get_race_context_map(db, race_ids):
    """Return race context keyed by race_id including track and race number."""
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
        }
    return context


def enrich_paper_bets(db, bets):
    """Attach display-friendly fields such as horse_name to paper bets."""
    runner_name_map = get_runner_name_map(db, [bet.runner_id for bet in bets])
    race_context_map = get_race_context_map(db, [bet.race_id for bet in bets])
    enriched = []

    for bet in bets:
        race_context = race_context_map.get(bet.race_id, {})
        enriched.append(
            {
                "id": bet.id,
                "horse_name": runner_name_map.get(bet.runner_id, f"Runner {bet.runner_id}"),
                "race_id": bet.race_id,
                "track": race_context.get("track"),
                "race_number": race_context.get("race_number"),
                "runner_id": bet.runner_id,
                "odds_taken": bet.odds_taken,
                "stake": bet.stake,
                "market_probability": bet.market_probability,
                "model_probability": bet.model_probability,
                "edge": bet.edge,
                "decision_reason": bet.decision_reason,
                "decision_version": bet.decision_version,
                "result": bet.result,
                "profit_loss": bet.profit_loss,
                "settled_flag": bet.settled_flag,
            }
        )

    return enriched
