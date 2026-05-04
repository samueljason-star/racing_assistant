import sys
from datetime import datetime, timezone
import json
from pathlib import Path
from zoneinfo import ZoneInfo

from flask import Flask, render_template_string

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.betting.bet_details import enrich_paper_bets
from app.betting.paper_bank import get_all_strategy_bank_summary
from app.config import ACTIVE_DECISION_VERSION, DASHBOARD_FOCUS_DECISION_VERSION
from app.db import SessionLocal, init_db
from app.models import Meeting, PaperBet, Race
from app.reports.performance import (
    build_edge_bucket_breakdown,
    build_odds_bucket_breakdown,
    build_performance_stats,
    build_version_breakdown,
)

BRISBANE_TZ = ZoneInfo("Australia/Brisbane")
MODEL_EDGE_V3_CANDIDATE_PATH = ROOT_DIR / "app" / "research" / "artifacts" / "model_edge_v3_candidate.json"

app = Flask(__name__)


HTML_TEMPLATE = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Racing Assistant Dashboard</title>
  <style>
    :root {
      --bg: #f4efe6;
      --card: rgba(255, 251, 245, 0.96);
      --ink: #1d1b18;
      --muted: #6d6457;
      --line: rgba(108, 92, 74, 0.18);
      --accent: #aa3d2a;
      --soft: rgba(170, 61, 42, 0.12);
      --success: #1b6b52;
      --danger: #9a2f2f;
      --shadow: 0 14px 40px rgba(76, 53, 33, 0.10);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      padding: 16px;
      color: var(--ink);
      font-family: Georgia, "Times New Roman", serif;
      background:
        radial-gradient(circle at top right, rgba(170, 61, 42, 0.12), transparent 28%),
        linear-gradient(180deg, #f8f3ea 0%, #efe5d5 100%);
    }
    .container { max-width: 1180px; margin: 0 auto; }
    .hero, .card, .bet-card {
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 20px;
      box-shadow: var(--shadow);
    }
    .hero {
      padding: 22px;
      margin-bottom: 16px;
      background: linear-gradient(135deg, rgba(170, 61, 42, 0.96), rgba(92, 36, 26, 0.92));
      color: #fff7ef;
    }
    .hero h1 { margin: 0; font-size: 2rem; }
    .hero p { margin: 8px 0 0; color: rgba(255, 247, 239, 0.84); }
    .stats, .grid-2, .grid-3 {
      display: grid;
      gap: 12px;
    }
    .stats { grid-template-columns: repeat(3, minmax(0, 1fr)); margin-bottom: 16px; }
    .grid-2 { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    .grid-3 { grid-template-columns: repeat(3, minmax(0, 1fr)); }
    .card, .bet-card { padding: 16px; }
    .label {
      color: var(--muted);
      font-size: 0.75rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      font-weight: 700;
      margin-bottom: 8px;
    }
    .value { font-size: 1.45rem; font-weight: 700; }
    .subvalue { margin-top: 6px; color: var(--muted); font-size: 0.92rem; }
    .section { margin-top: 18px; }
    .section h2 { margin: 0 0 10px; font-size: 1.08rem; }
    .table { display: grid; gap: 8px; }
    .row {
      display: grid;
      grid-template-columns: 1.6fr repeat(6, minmax(0, 1fr));
      gap: 10px;
      padding: 9px 0;
      border-top: 1px solid var(--line);
      font-size: 0.92rem;
    }
    .row.header {
      border-top: none;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.06em;
      font-size: 0.72rem;
      font-weight: 700;
      padding-top: 0;
    }
    .bet-list { display: grid; gap: 12px; }
    .bet-title {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 8px;
      font-size: 1rem;
      font-weight: 700;
    }
    .bet-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 8px 14px;
      margin-top: 10px;
    }
    .bet-line, .bet-full {
      color: var(--muted);
      font-size: 0.92rem;
      line-height: 1.45;
      word-break: break-word;
    }
    .bet-full { margin-top: 10px; padding-top: 10px; border-top: 1px solid var(--line); }
    .pill {
      padding: 4px 9px;
      border-radius: 999px;
      background: var(--soft);
      color: var(--accent);
      font-size: 0.75rem;
      font-weight: 700;
      white-space: nowrap;
    }
    .positive { color: var(--success); }
    .negative { color: var(--danger); }
    .empty { color: var(--muted); font-size: 0.95rem; }
    @media (max-width: 900px) {
      .stats, .grid-3 { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .grid-2 { grid-template-columns: 1fr; }
      .row { grid-template-columns: 1.4fr repeat(3, minmax(0, 1fr)); }
      .hide-sm { display: none; }
    }
    @media (max-width: 640px) {
      body { padding: 14px; }
      .hero h1 { font-size: 1.6rem; }
      .stats { grid-template-columns: 1fr 1fr; }
      .bet-grid { grid-template-columns: 1fr; }
      .row { grid-template-columns: 1.3fr 1fr 1fr; }
      .hide-md { display: none; }
      .value { font-size: 1.2rem; }
    }
  </style>
</head>
<body>
  <div class="container">
    <div class="hero">
      <h1>Racing Assistant</h1>
      <p>Validation-led paper betting with version-separated banks, CLV tracking, and safer model_edge_v3 monitoring.</p>
    </div>

    <div class="stats">
      <div class="card"><div class="label">{{ focus_version }} Bank</div><div class="value">${{ focus_bank }}</div></div>
      <div class="card"><div class="label">{{ focus_version }} Daily P/L</div><div class="value {% if daily_pl < 0 %}negative{% else %}positive{% endif %}">${{ daily_pl_fmt }}</div></div>
      <div class="card"><div class="label">{{ focus_version }} ROI</div><div class="value">{{ focus_roi }}</div></div>
      <div class="card"><div class="label">{{ focus_version }} Open Bets</div><div class="value">{{ open_bets }}</div></div>
      <div class="card"><div class="label">{{ focus_version }} Settled Bets</div><div class="value">{{ settled_bets }}</div></div>
      <div class="card"><div class="label">{{ focus_version }} P/L</div><div class="value {% if focus_profit_loss < 0 %}negative{% else %}positive{% endif %}">${{ focus_profit_loss_fmt }}</div></div>
    </div>

    {% if validation_warnings %}
    <div class="section">
      <h2>Validation Warnings</h2>
      <div class="card">
        <div class="table">
          {% for warning in validation_warnings %}
          <div class="row" style="grid-template-columns: 1fr;">
            <div>{{ warning }}</div>
          </div>
          {% endfor %}
        </div>
      </div>
    </div>
    {% endif %}

    <div class="section">
      <h2>{{ focus_version }} Bank Summary</h2>
      <div class="card">
        <div class="table">
          <div class="row header">
            <div>Version</div>
            <div>Start</div>
            <div>Bank</div>
            <div class="hide-md">P/L</div>
            <div class="hide-sm">ROI</div>
            <div class="hide-sm">Open</div>
            <div class="hide-sm">Settled</div>
          </div>
          {% for item in focus_summary %}
          <div class="row">
            <div>{{ item.decision_version }}</div>
            <div>${{ "%.2f"|format(item.starting_bank) }}</div>
            <div>${{ "%.2f"|format(item.current_bank) }}</div>
            <div class="hide-md">${{ "%.2f"|format(item.profit_loss) }}</div>
            <div class="hide-sm">{{ "%.2f%%"|format(item.roi * 100) }}</div>
            <div class="hide-sm">{{ item.open_bets }}</div>
            <div class="hide-sm">{{ item.settled_bets }}</div>
          </div>
          {% endfor %}
        </div>
      </div>
    </div>

    <div class="section">
      <h2>Legacy Strategy Banks</h2>
      <div class="card">
        <div class="table">
          <div class="row header">
            <div>Version</div>
            <div>Start</div>
            <div>Bank</div>
            <div class="hide-md">P/L</div>
            <div class="hide-sm">ROI</div>
            <div class="hide-sm">Open</div>
            <div class="hide-sm">Settled</div>
          </div>
          {% for item in legacy_summary %}
          <div class="row">
            <div>{{ item.decision_version }}</div>
            <div>${{ "%.2f"|format(item.starting_bank) }}</div>
            <div>${{ "%.2f"|format(item.current_bank) }}</div>
            <div class="hide-md">${{ "%.2f"|format(item.profit_loss) }}</div>
            <div class="hide-sm">{{ "%.2f%%"|format(item.roi * 100) }}</div>
            <div class="hide-sm">{{ item.open_bets }}</div>
            <div class="hide-sm">{{ item.settled_bets }}</div>
          </div>
          {% endfor %}
        </div>
      </div>
    </div>

    <div class="section">
      <h2>Active Version Performance</h2>
      <div class="grid-3">
        {% for version, stats in version_stats.items() %}
        <div class="card">
          <div class="label">{{ version }}</div>
          <div class="subvalue">Bets {{ stats.total_bets }} | Open {{ stats.open_bets }} | Settled {{ stats.settled_bets }}</div>
          <div class="subvalue">Wins {{ stats.wins }} | Losses {{ stats.losses }} | Strike {{ "%.2f%%"|format(stats.strike_rate * 100) }}</div>
          <div class="subvalue">P/L ${{ "%.2f"|format(stats.profit_loss) }} | ROI {{ "%.2f%%"|format(stats.roi * 100) }}</div>
          <div class="subvalue">Avg Odds {{ "%.2f"|format(stats.avg_odds) }} | Avg Edge {{ "%.4f"|format(stats.avg_edge) }} | Avg CLV {{ "%+.2f"|format(stats.avg_clv) }}%</div>
        </div>
        {% endfor %}
      </div>
    </div>

    <div class="section">
      <h2>Performance Buckets</h2>
      <div class="grid-2">
        <div class="card">
          <div class="label">Odds Buckets</div>
          <div class="table">
            <div class="row header">
              <div>Bucket</div>
              <div>Count</div>
              <div>ROI</div>
              <div class="hide-md">Avg Odds</div>
              <div class="hide-sm">Avg Edge</div>
              <div class="hide-sm">Avg CLV</div>
              <div class="hide-sm">Open</div>
            </div>
            {% for bucket, stats in odds_bucket_stats.items() %}
            <div class="row">
              <div>{{ bucket }}</div>
              <div>{{ stats.total_bets }}</div>
              <div>{{ "%.2f%%"|format(stats.roi * 100) }}</div>
              <div class="hide-md">{{ "%.2f"|format(stats.avg_odds) }}</div>
              <div class="hide-sm">{{ "%.4f"|format(stats.avg_edge) }}</div>
              <div class="hide-sm">{{ "%+.2f"|format(stats.avg_clv) }}%</div>
              <div class="hide-sm">{{ stats.open_bets }}</div>
            </div>
            {% endfor %}
          </div>
        </div>
        <div class="card">
          <div class="label">Edge Buckets</div>
          <div class="table">
            <div class="row header">
              <div>Bucket</div>
              <div>Count</div>
              <div>ROI</div>
              <div class="hide-md">Avg Odds</div>
              <div class="hide-sm">Avg Edge</div>
              <div class="hide-sm">Avg CLV</div>
              <div class="hide-sm">Open</div>
            </div>
            {% for bucket, stats in edge_bucket_stats.items() %}
            <div class="row">
              <div>{{ bucket }}</div>
              <div>{{ stats.total_bets }}</div>
              <div>{{ "%.2f%%"|format(stats.roi * 100) }}</div>
              <div class="hide-md">{{ "%.2f"|format(stats.avg_odds) }}</div>
              <div class="hide-sm">{{ "%.4f"|format(stats.avg_edge) }}</div>
              <div class="hide-sm">{{ "%+.2f"|format(stats.avg_clv) }}%</div>
              <div class="hide-sm">{{ stats.open_bets }}</div>
            </div>
            {% endfor %}
          </div>
        </div>
      </div>
    </div>

    <div class="section">
      <h2>Recent Bets</h2>
      <div class="bet-list">
        {% for bet in recent_bets %}
        <div class="bet-card">
          <div class="bet-title">
            <span>{{ bet.horse_name }}</span>
            <span class="pill">{{ bet.decision_version }}</span>
          </div>
          <div class="bet-grid">
            <div class="bet-line">Track: {{ bet.track or "Unknown" }} R{{ bet.race_number or "?" }}</div>
            <div class="bet-line">Odds Taken: {{ "%.2f"|format(bet.odds_taken) }}</div>
            <div class="bet-line">Stake: ${{ "%.2f"|format(bet.stake) }}</div>
            <div class="bet-line">Model Prob: {{ "%.4f"|format(bet.model_probability) }}</div>
            <div class="bet-line">Raw Market Prob: {{ "%.4f"|format(bet.raw_market_probability or 0) }}</div>
            <div class="bet-line">Adj Market Prob: {{ "%.4f"|format(bet.market_probability) }}</div>
            <div class="bet-line">Edge: {{ "%.4f"|format(bet.edge) }}</div>
            <div class="bet-line">Form Score: {{ "%.4f"|format(bet.form_score or 0) }}</div>
            <div class="bet-line">Combined Score: {{ "%.4f"|format(bet.combined_score or 0) }}</div>
            <div class="bet-line">Recent Form: {{ bet.qualification_reason or "N/A" }}</div>
            <div class="bet-line">Result: {{ bet.result or "OPEN" }}</div>
            <div class="bet-line">Profit/Loss: ${{ "%.2f"|format(bet.profit_loss or 0) }}</div>
            <div class="bet-line">Final Odds: {{ "%.2f"|format(bet.final_observed_odds or 0) if bet.final_observed_odds is not none else "N/A" }}</div>
            <div class="bet-line">CLV Percent: {{ ("%+.2f"|format(bet.clv_percent)) ~ "%" if bet.clv_percent is not none else "N/A" }}</div>
            <div class="bet-line">Last Start Finish: {{ bet.last_start_finish if bet.last_start_finish is not none else "N/A" }}</div>
            <div class="bet-line">Avg Last 3 Finish: {{ "%.2f"|format(bet.avg_last3_finish) if bet.avg_last3_finish is not none else "N/A" }}</div>
            <div class="bet-line">Avg Margin: {{ "%.2f"|format(bet.avg_last3_margin) if bet.avg_last3_margin is not none else "N/A" }}</div>
          </div>
          <div class="bet-full">Decision Reason: {{ bet.decision_reason or "N/A" }}</div>
        </div>
        {% else %}
        <div class="empty">No paper bets found.</div>
        {% endfor %}
      </div>
    </div>
  </div>
</body>
</html>
"""


def _meeting_date_map(db, race_ids):
    if not race_ids:
        return {}

    races = db.query(Race).filter(Race.id.in_(sorted(set(race_ids)))).all()
    meeting_ids = [race.meeting_id for race in races if race.meeting_id is not None]
    meetings = db.query(Meeting).filter(Meeting.id.in_(sorted(set(meeting_ids)))).all()
    meeting_map = {meeting.id: meeting.date for meeting in meetings}
    return {race.id: meeting_map.get(race.meeting_id) for race in races}


def _to_brisbane(value):
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(BRISBANE_TZ)


def _is_same_brisbane_day(value, target_date) -> bool:
    converted = _to_brisbane(value)
    return bool(converted and converted.date() == target_date)


def _load_focus_warnings() -> list[str]:
    if not MODEL_EDGE_V3_CANDIDATE_PATH.exists():
        return []
    try:
        payload = json.loads(MODEL_EDGE_V3_CANDIDATE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return ["validation_artifact_unreadable"]
    return [str(item) for item in payload.get("warnings", [])]


@app.route("/")
def index():
    init_db()
    db = SessionLocal()

    try:
        all_rows = db.query(PaperBet).order_by(PaperBet.id.desc()).all()
        focus_version = DASHBOARD_FOCUS_DECISION_VERSION
        summary_all = get_all_strategy_bank_summary(db)
        known_versions = {item["decision_version"] for item in summary_all}
        if focus_version not in known_versions:
            focus_version = ACTIVE_DECISION_VERSION

        active_rows = [
            bet for bet in all_rows
            if (bet.decision_version or ACTIVE_DECISION_VERSION) == focus_version
        ]
        active_bets = enrich_paper_bets(db, active_rows)
        open_bets = [bet for bet in active_bets if not bet["settled_flag"]]
        settled_bets = [bet for bet in active_bets if bet["settled_flag"]]
        strategy_summary = summary_all
        summary_by_version = {
            item["decision_version"]: item
            for item in strategy_summary
        }
        current_date = datetime.now(BRISBANE_TZ).date()
        todays_bets = [bet for bet in active_bets if _is_same_brisbane_day(bet.get("placed_at"), current_date)]
        settled_today = [
            bet for bet in active_bets
            if bet["settled_flag"] and _is_same_brisbane_day(bet.get("settled_at"), current_date)
        ]
        todays_stats = build_performance_stats(todays_bets)
        settled_today_stats = build_performance_stats(settled_today)
        todays_stats["profit_loss"] = settled_today_stats["profit_loss"]
        version_stats = build_version_breakdown(active_bets)
        odds_bucket_stats = build_odds_bucket_breakdown(active_bets)
        edge_bucket_stats = build_edge_bucket_breakdown(active_bets)
        focus_summary_row = summary_by_version.get(focus_version, {})
        focus_summary = [item for item in strategy_summary if item["decision_version"] == focus_version]
        legacy_summary = [item for item in strategy_summary if item["decision_version"] != focus_version]

        return render_template_string(
            HTML_TEMPLATE,
            focus_version=focus_version,
            focus_bank=f"{focus_summary_row.get('current_bank', 0.0):.2f}",
            focus_profit_loss=focus_summary_row.get("profit_loss", 0.0),
            focus_profit_loss_fmt=f"{focus_summary_row.get('profit_loss', 0.0):.2f}",
            focus_roi=f"{focus_summary_row.get('roi', 0.0):.2%}",
            daily_pl=todays_stats["profit_loss"],
            daily_pl_fmt=f"{todays_stats['profit_loss']:.2f}",
            open_bets=len(open_bets),
            settled_bets=len(settled_bets),
            focus_summary=focus_summary,
            legacy_summary=legacy_summary,
            version_stats=version_stats,
            odds_bucket_stats=odds_bucket_stats,
            edge_bucket_stats=edge_bucket_stats,
            validation_warnings=_load_focus_warnings(),
            recent_bets=active_bets[:20],
        )
    finally:
        db.close()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=False)
