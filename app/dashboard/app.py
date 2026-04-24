import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from flask import Flask, render_template_string

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.betting.bet_details import enrich_paper_bets
from app.betting.paper_bank import (
    ACTIVE_DECISION_VERSION,
    get_all_strategy_bank_summary,
    get_combined_bank,
)
from app.db import SessionLocal, init_db
from app.models import Meeting, PaperBet, Race
from app.reports.performance import (
    build_edge_bucket_breakdown,
    build_odds_bucket_breakdown,
    build_performance_stats,
    build_version_breakdown,
)

BRISBANE_TZ = ZoneInfo("Australia/Brisbane")

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
      <p>Version-separated v2 edge betting with commission-adjusted probabilities, CLV tracking, and cleaner strategy analysis.</p>
    </div>

    <div class="stats">
      <div class="card"><div class="label">Combined Bank</div><div class="value">${{ combined_bank }}</div></div>
      <div class="card"><div class="label">model_edge_v2 Bank</div><div class="value">${{ v2_bank }}</div></div>
      <div class="card"><div class="label">model_edge_v2 P/L</div><div class="value {% if v2_profit_loss < 0 %}negative{% else %}positive{% endif %}">${{ v2_profit_loss_fmt }}</div></div>
      <div class="card"><div class="label">model_edge_v2 ROI</div><div class="value">{{ v2_roi }}</div></div>
      <div class="card"><div class="label">Daily P/L</div><div class="value {% if daily_pl < 0 %}negative{% else %}positive{% endif %}">${{ daily_pl_fmt }}</div></div>
      <div class="card"><div class="label">Open Bets</div><div class="value">{{ open_bets }}</div></div>
    </div>

    <div class="section">
      <h2>Strategy Banks</h2>
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
          {% for item in strategy_summary %}
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
      <h2>Performance By Version</h2>
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
            <div class="bet-line">Result: {{ bet.result or "OPEN" }}</div>
            <div class="bet-line">Profit/Loss: ${{ "%.2f"|format(bet.profit_loss or 0) }}</div>
            <div class="bet-line">Final Odds: {{ "%.2f"|format(bet.final_observed_odds or 0) if bet.final_observed_odds is not none else "N/A" }}</div>
            <div class="bet-line">CLV Percent: {{ ("%+.2f"|format(bet.clv_percent)) ~ "%" if bet.clv_percent is not none else "N/A" }}</div>
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


@app.route("/")
def index():
    init_db()
    db = SessionLocal()

    try:
        all_rows = db.query(PaperBet).order_by(PaperBet.id.desc()).all()
        all_bets = enrich_paper_bets(db, all_rows)
        open_bets = [bet for bet in all_bets if not bet["settled_flag"]]
        strategy_summary = get_all_strategy_bank_summary(db)
        summary_by_version = {
            item["decision_version"]: item
            for item in strategy_summary
        }
        current_date = datetime.now(BRISBANE_TZ).date().isoformat()
        race_date_map = _meeting_date_map(db, [bet["race_id"] for bet in all_bets])
        todays_bets = [bet for bet in all_bets if race_date_map.get(bet["race_id"]) == current_date]
        todays_stats = build_performance_stats(todays_bets)
        version_stats = build_version_breakdown(all_bets)
        odds_bucket_stats = build_odds_bucket_breakdown(all_bets)
        edge_bucket_stats = build_edge_bucket_breakdown(all_bets)
        v2_summary = summary_by_version.get(ACTIVE_DECISION_VERSION, {})

        return render_template_string(
            HTML_TEMPLATE,
            combined_bank=f"{get_combined_bank(db):.2f}",
            v2_bank=f"{v2_summary.get('current_bank', 0.0):.2f}",
            v2_profit_loss=v2_summary.get("profit_loss", 0.0),
            v2_profit_loss_fmt=f"{v2_summary.get('profit_loss', 0.0):.2f}",
            v2_roi=f"{v2_summary.get('roi', 0.0):.2%}",
            daily_pl=todays_stats["profit_loss"],
            daily_pl_fmt=f"{todays_stats['profit_loss']:.2f}",
            open_bets=len(open_bets),
            strategy_summary=strategy_summary,
            version_stats=version_stats,
            odds_bucket_stats=odds_bucket_stats,
            edge_bucket_stats=edge_bucket_stats,
            recent_bets=all_bets[:20],
        )
    finally:
        db.close()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=False)
