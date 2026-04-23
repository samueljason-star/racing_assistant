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
    STARTING_BANK,
    get_bank_baseline,
    get_bank_since_reset,
    get_current_bank,
    get_lifetime_bank,
    get_total_roi,
    get_latest_reset,
)
from app.config import ACTIVE_DECISION_VERSION
from app.db import SessionLocal, init_db
from app.models import Meeting, PaperBet, Race
from app.reports.calibration_utils import collect_calibration_rows, summarize_calibration
from app.reports.performance import (
    build_label_breakdown,
    build_odds_bucket_breakdown,
    build_status_breakdown,
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
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    :root {
      --page: #f4efe6;
      --card: rgba(255, 251, 245, 0.94);
      --ink: #1d1b18;
      --muted: #6d6457;
      --line: rgba(108, 92, 74, 0.18);
      --accent: #aa3d2a;
      --accent-soft: rgba(170, 61, 42, 0.12);
      --success: #1b6b52;
      --danger: #9a2f2f;
      --shadow: 0 14px 40px rgba(76, 53, 33, 0.10);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      padding: 18px;
      color: var(--ink);
      font-family: Georgia, "Times New Roman", serif;
      background:
        radial-gradient(circle at top right, rgba(170, 61, 42, 0.12), transparent 28%),
        linear-gradient(180deg, #f8f3ea 0%, #efe5d5 100%);
    }
    .container { max-width: 1120px; margin: 0 auto; }
    .hero {
      padding: 20px 22px;
      border-radius: 24px;
      background: linear-gradient(135deg, rgba(170, 61, 42, 0.95), rgba(92, 36, 26, 0.92));
      color: #fff8ef;
      box-shadow: var(--shadow);
      margin-bottom: 18px;
    }
    .hero h1 { margin: 0; font-size: 2rem; }
    .hero p { margin: 8px 0 0; color: rgba(255, 248, 239, 0.82); }
    .stats {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 18px;
    }
    .card {
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 20px;
      padding: 16px;
      box-shadow: var(--shadow);
      backdrop-filter: blur(4px);
    }
    .label {
      margin: 0 0 8px;
      color: var(--muted);
      font-size: 0.75rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      font-weight: 700;
    }
    .value {
      font-size: 1.45rem;
      font-weight: 700;
    }
    .subvalue {
      margin-top: 6px;
      color: var(--muted);
      font-size: 0.9rem;
    }
    .section {
      margin-top: 18px;
    }
    .section h2 {
      margin: 0 0 10px;
      font-size: 1.1rem;
    }
    .grid-2, .grid-3 {
      display: grid;
      gap: 12px;
    }
    .grid-2 { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    .grid-3 { grid-template-columns: repeat(3, minmax(0, 1fr)); }
    .table {
      display: grid;
      gap: 10px;
    }
    .row {
      display: grid;
      grid-template-columns: 1.3fr repeat(5, minmax(0, 1fr));
      gap: 10px;
      padding: 10px 0;
      border-top: 1px solid var(--line);
      font-size: 0.92rem;
    }
    .row.header {
      border-top: none;
      padding-top: 0;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.06em;
      font-size: 0.72rem;
      font-weight: 700;
    }
    .bet-list { display: grid; gap: 12px; }
    .bet-card {
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 20px;
      padding: 16px;
      box-shadow: var(--shadow);
    }
    .bet-title {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 8px;
      font-size: 1rem;
      font-weight: 700;
    }
    .pill {
      padding: 4px 9px;
      border-radius: 999px;
      background: var(--accent-soft);
      color: var(--accent);
      font-size: 0.75rem;
      font-weight: 700;
      white-space: nowrap;
    }
    .bet-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 8px 14px;
      margin-top: 10px;
    }
    .bet-line {
      color: var(--muted);
      font-size: 0.92rem;
      line-height: 1.4;
      word-break: break-word;
    }
    .bet-full {
      margin-top: 10px;
      padding-top: 10px;
      border-top: 1px solid var(--line);
      color: var(--muted);
      font-size: 0.92rem;
      line-height: 1.45;
    }
    .positive { color: var(--success); }
    .negative { color: var(--danger); }
    .chart-card {
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 20px;
      padding: 14px;
      box-shadow: var(--shadow);
    }
    .empty {
      color: var(--muted);
      font-size: 0.95rem;
    }
    @media (max-width: 900px) {
      .stats, .grid-3 { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .grid-2 { grid-template-columns: 1fr; }
      .row { grid-template-columns: 1.2fr repeat(3, minmax(0, 1fr)); }
      .row .hide-sm { display: none; }
    }
    @media (max-width: 640px) {
      body { padding: 14px; }
      .hero h1 { font-size: 1.6rem; }
      .stats { grid-template-columns: 1fr 1fr; }
      .bet-grid { grid-template-columns: 1fr; }
      .value { font-size: 1.2rem; }
      .row { grid-template-columns: 1.2fr 1fr 1fr; }
      .row .hide-md { display: none; }
    }
  </style>
</head>
<body>
  <div class="container">
    <div class="hero">
      <h1>Racing Assistant</h1>
      <p>Paper betting dashboard for {{ active_version }} with commission-aware edge, CLV tracking, and segmented performance.</p>
    </div>

    <div class="stats">
      <div class="card">
        <div class="label">Overall Bank</div>
        <div class="value">${{ lifetime_bank }}</div>
        <div class="subvalue">Lifetime-equivalent view</div>
      </div>
      <div class="card">
        <div class="label">Current Bank</div>
        <div class="value">${{ current_bank }}</div>
        <div class="subvalue">Baseline ${{ reset_baseline }} since {{ reset_date }}</div>
      </div>
      <div class="card">
        <div class="label">Bank Since Reset</div>
        <div class="value {% if bank_since_reset_value < 0 %}negative{% else %}positive{% endif %}">${{ bank_since_reset }}</div>
        <div class="subvalue">Active cycle P/L</div>
      </div>
      <div class="card">
        <div class="label">Total ROI</div>
        <div class="value">{{ total_roi }}</div>
        <div class="subvalue">Settled active bets</div>
      </div>
      <div class="card">
        <div class="label">Bets Today</div>
        <div class="value">{{ bets_today }}</div>
        <div class="subvalue">Open + settled</div>
      </div>
      <div class="card">
        <div class="label">Open Bets</div>
        <div class="value">{{ open_bets }}</div>
        <div class="subvalue">Exposure ${{ open_exposure }}</div>
      </div>
      <div class="card">
        <div class="label">Daily P/L</div>
        <div class="value {% if daily_pl_value < 0 %}negative{% else %}positive{% endif %}">${{ daily_pl }}</div>
        <div class="subvalue">Today settled only</div>
      </div>
      <div class="card">
        <div class="label">Calibration</div>
        <div class="value">{{ calibration_brier }}</div>
        <div class="subvalue">Brier score</div>
      </div>
    </div>

    <div class="section">
      <h2>Bank Since Reset</h2>
      <div class="chart-card">
        <canvas id="bankChart"></canvas>
      </div>
    </div>

    <div class="section">
      <h2>Status And Calibration</h2>
      <div class="grid-2">
        <div class="card">
          <div class="label">Open Vs Settled</div>
          <div class="table">
            <div class="row header">
              <div>Status</div>
              <div>Count</div>
              <div>ROI</div>
              <div class="hide-md">P/L</div>
              <div class="hide-sm">CLV</div>
              <div class="hide-sm">Beat Rate</div>
            </div>
            <div class="row">
              <div>Open</div>
              <div>{{ status_breakdown.open.total_bets }}</div>
              <div>N/A</div>
              <div class="hide-md">Exposure ${{ "%.2f"|format(status_breakdown.open.stake_exposure) }}</div>
              <div class="hide-sm">N/A</div>
              <div class="hide-sm">N/A</div>
            </div>
            <div class="row">
              <div>Settled</div>
              <div>{{ status_breakdown.settled.total_bets }}</div>
              <div>{{ "%.2f%%"|format(status_breakdown.settled.roi * 100) }}</div>
              <div class="hide-md">${{ "%.2f"|format(status_breakdown.settled.profit_loss) }}</div>
              <div class="hide-sm">{{ "%+.4f"|format(status_breakdown.settled.avg_clv_diff) }}</div>
              <div class="hide-sm">{{ "%.2f%%"|format(status_breakdown.settled.beat_clv_rate * 100) }}</div>
            </div>
          </div>
        </div>
        <div class="card">
          <div class="label">Calibration Summary</div>
          <div class="subvalue">Rows scored: {{ calibration_rows }}</div>
          <div class="subvalue">Buckets: {{ calibration_bucket_count }}</div>
          <div class="table">
            <div class="row header">
              <div>Bucket</div>
              <div>Count</div>
              <div>Pred</div>
              <div class="hide-md">Actual</div>
              <div class="hide-sm">Market</div>
              <div class="hide-sm">Edge</div>
            </div>
            {% for bucket in calibration_buckets %}
            <div class="row">
              <div>{{ bucket.bucket }}</div>
              <div>{{ bucket.count }}</div>
              <div>{{ "%.3f"|format(bucket.avg_predicted_probability) }}</div>
              <div class="hide-md">{{ "%.3f"|format(bucket.actual_win_rate) }}</div>
              <div class="hide-sm">{{ "%.3f"|format(bucket.avg_market_probability or 0) }}</div>
              <div class="hide-sm">{{ "%.3f"|format(bucket.avg_edge or 0) }}</div>
            </div>
            {% else %}
            <div class="empty">No calibration data yet.</div>
            {% endfor %}
          </div>
        </div>
      </div>
    </div>

    <div class="section">
      <h2>Performance By Strategy Version</h2>
      <div class="grid-3">
        {% for version, stats in version_stats.items() %}
        <div class="card">
          <div class="label">{{ version }}</div>
          <div class="subvalue">Bets {{ stats.total_bets }} | Wins {{ stats.wins }} | Losses {{ stats.losses }}</div>
          <div class="subvalue">P/L ${{ "%.2f"|format(stats.profit_loss) }} | ROI {{ "%.2f%%"|format(stats.roi * 100) }}</div>
          <div class="subvalue">Avg CLV {{ "%+.4f"|format(stats.avg_clv_diff) }} | Beat Rate {{ "%.2f%%"|format(stats.beat_clv_rate * 100) }}</div>
        </div>
        {% else %}
        <div class="empty">No settled bets yet.</div>
        {% endfor %}
      </div>
    </div>

    <div class="section">
      <h2>Performance By Odds Bucket</h2>
      <div class="card">
        <div class="table">
          <div class="row header">
            <div>Bucket</div>
            <div>Count</div>
            <div>Wins</div>
            <div class="hide-md">P/L</div>
            <div class="hide-sm">ROI</div>
            <div class="hide-sm">CLV</div>
          </div>
          {% for bucket, stats in odds_bucket_stats.items() %}
          <div class="row">
            <div>{{ bucket }}</div>
            <div>{{ stats.total_bets }}</div>
            <div>{{ stats.wins }}</div>
            <div class="hide-md">${{ "%.2f"|format(stats.profit_loss) }}</div>
            <div class="hide-sm">{{ "%.2f%%"|format(stats.roi * 100) }}</div>
            <div class="hide-sm">{{ "%+.4f"|format(stats.avg_clv_diff) }}</div>
          </div>
          {% else %}
          <div class="empty">No settled bets yet.</div>
          {% endfor %}
        </div>
      </div>
    </div>

    <div class="section">
      <h2>Track And Race Type Segments</h2>
      <div class="grid-2">
        <div class="card">
          <div class="label">Top Tracks</div>
          <div class="table">
            <div class="row header">
              <div>Track</div>
              <div>Count</div>
              <div>Wins</div>
              <div class="hide-md">P/L</div>
              <div class="hide-sm">ROI</div>
              <div class="hide-sm">CLV</div>
            </div>
            {% for track, stats in track_stats.items() %}
            <div class="row">
              <div>{{ track }}</div>
              <div>{{ stats.total_bets }}</div>
              <div>{{ stats.wins }}</div>
              <div class="hide-md">${{ "%.2f"|format(stats.profit_loss) }}</div>
              <div class="hide-sm">{{ "%.2f%%"|format(stats.roi * 100) }}</div>
              <div class="hide-sm">{{ "%+.4f"|format(stats.avg_clv_diff) }}</div>
            </div>
            {% else %}
            <div class="empty">No settled bets yet.</div>
            {% endfor %}
          </div>
        </div>
        <div class="card">
          <div class="label">Top Race Types</div>
          <div class="table">
            <div class="row header">
              <div>Race Type</div>
              <div>Count</div>
              <div>Wins</div>
              <div class="hide-md">P/L</div>
              <div class="hide-sm">ROI</div>
              <div class="hide-sm">CLV</div>
            </div>
            {% for race_type, stats in race_type_stats.items() %}
            <div class="row">
              <div>{{ race_type }}</div>
              <div>{{ stats.total_bets }}</div>
              <div>{{ stats.wins }}</div>
              <div class="hide-md">${{ "%.2f"|format(stats.profit_loss) }}</div>
              <div class="hide-sm">{{ "%.2f%%"|format(stats.roi * 100) }}</div>
              <div class="hide-sm">{{ "%+.4f"|format(stats.avg_clv_diff) }}</div>
            </div>
            {% else %}
            <div class="empty">No settled bets yet.</div>
            {% endfor %}
          </div>
        </div>
      </div>
    </div>

    <div class="section">
      <h2>Open Bets</h2>
      <div class="bet-list">
        {% for bet in recent_open_bets %}
        <div class="bet-card">
          <div class="bet-title">
            <span>{{ bet.horse_name }}</span>
            <span class="pill">{{ bet.decision_version or "N/A" }}</span>
          </div>
          <div class="bet-grid">
            <div class="bet-line">Track: {{ bet.track or "Unknown" }} R{{ bet.race_number or "?" }}</div>
            <div class="bet-line">Race Type: {{ bet.race_type or bet.meeting_type or "Unknown" }}</div>
            <div class="bet-line">Odds Taken: {{ "%.2f"|format(bet.odds_taken) }}</div>
            <div class="bet-line">Stake: ${{ "%.2f"|format(bet.stake) }}</div>
            <div class="bet-line">Odds Bucket: {{ bet.odds_bucket }}</div>
            <div class="bet-line">Adj Market Prob: {{ "%.4f"|format(bet.market_probability) }}</div>
            <div class="bet-line">Model Prob: {{ "%.4f"|format(bet.model_probability) }}</div>
            <div class="bet-line">Edge: {{ "%.4f"|format(bet.edge) }}</div>
            <div class="bet-line">Commission: {{ "%.2f%%"|format((bet.commission_rate or 0) * 100) }}</div>
          </div>
          <div class="bet-full">Decision Reason: {{ bet.decision_reason or "N/A" }}</div>
        </div>
        {% else %}
        <div class="empty">No open paper bets found.</div>
        {% endfor %}
      </div>
    </div>

    <div class="section">
      <h2>Recent Settled Bets</h2>
      <div class="bet-list">
        {% for bet in recent_settled_bets %}
        <div class="bet-card">
          <div class="bet-title">
            <span>{{ bet.horse_name }}</span>
            <span class="pill">{{ bet.result or "SETTLED" }}</span>
          </div>
          <div class="bet-grid">
            <div class="bet-line">Track: {{ bet.track or "Unknown" }} R{{ bet.race_number or "?" }}</div>
            <div class="bet-line">Race Type: {{ bet.race_type or bet.meeting_type or "Unknown" }}</div>
            <div class="bet-line">Version: {{ bet.decision_version or "N/A" }}</div>
            <div class="bet-line">Odds Taken: {{ "%.2f"|format(bet.odds_taken) }}</div>
            <div class="bet-line">Final Odds: {{ "%.2f"|format(bet.final_observed_odds or 0) if bet.final_observed_odds is not none else "N/A" }}</div>
            <div class="bet-line">CLV Diff: {{ "%+.2f"|format(bet.closing_line_difference or 0) if bet.closing_line_difference is not none else "N/A" }}</div>
            <div class="bet-line">Beat Closing Line: {{ bet.beat_closing_line if bet.beat_closing_line is not none else "N/A" }}</div>
            <div class="bet-line">Stake: ${{ "%.2f"|format(bet.stake) }}</div>
            <div class="bet-line">Profit/Loss: ${{ "%.2f"|format(bet.profit_loss or 0) }}</div>
          </div>
          <div class="bet-full">Decision Reason: {{ bet.decision_reason or "N/A" }}</div>
        </div>
        {% else %}
        <div class="empty">No settled bets found.</div>
        {% endfor %}
      </div>
    </div>
  </div>

  <script>
    const labels = {{ bank_chart_labels|tojson }};
    const values = {{ bank_chart_values|tojson }};

    new Chart(document.getElementById("bankChart"), {
      type: "line",
      data: {
        labels: labels,
        datasets: [{
          data: values,
          borderColor: "#aa3d2a",
          backgroundColor: "rgba(170, 61, 42, 0.12)",
          fill: true,
          tension: 0.28,
          pointRadius: 2
        }]
      },
      options: {
        responsive: true,
        plugins: { legend: { display: false } },
        scales: { y: { beginAtZero: false } }
      }
    });
  </script>
</body>
</html>
"""


def _bank_series(settled_bets, baseline):
    labels = ["Reset"]
    values = [baseline]
    running_bank = baseline

    for index, bet in enumerate(settled_bets, start=1):
        running_bank += bet.profit_loss or 0.0
        labels.append(f"Bet {index}")
        values.append(round(running_bank, 2))

    return labels, values


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
        all_bets = db.query(PaperBet).order_by(PaperBet.id.desc()).all()
        settled_bets = [
            bet
            for bet in reversed(all_bets)
            if bet.settled_flag and bet.profit_loss is not None
        ]
        open_bet_rows = [bet for bet in all_bets if not bet.settled_flag]

        recent_open_bets = enrich_paper_bets(db, open_bet_rows[:20])
        recent_settled_bets = enrich_paper_bets(
            db,
            [bet for bet in all_bets if bet.settled_flag][:20],
        )
        enriched_all_bets = enrich_paper_bets(db, all_bets)

        today = datetime.now(BRISBANE_TZ).date().isoformat()
        race_date_map = _meeting_date_map(db, [bet.race_id for bet in all_bets])
        bets_today_rows = [bet for bet in all_bets if race_date_map.get(bet.race_id) == today]
        settled_today_rows = [
            bet for bet in bets_today_rows if bet.settled_flag and bet.profit_loss is not None
        ]

        current_bank = get_current_bank(db)
        lifetime_bank = get_lifetime_bank(db)
        bank_since_reset = get_bank_since_reset(db)
        total_roi = get_total_roi(db)
        daily_pl = round(sum((bet.profit_loss or 0.0) for bet in settled_today_rows), 2)
        bets_today = len(bets_today_rows)
        open_bets = len(open_bet_rows)
        reset_baseline = get_bank_baseline(db)
        latest_reset = get_latest_reset(db)
        reset_date = (
            latest_reset.reset_at.strftime("%Y-%m-%d %H:%M")
            if latest_reset and latest_reset.reset_at is not None
            else "Initial"
        )
        bank_chart_labels, bank_chart_values = _bank_series(settled_bets, reset_baseline)
        version_stats = build_version_breakdown(settled_bets)
        odds_bucket_stats = build_odds_bucket_breakdown(settled_bets)
        status_breakdown = build_status_breakdown(open_bet_rows, settled_bets)
        track_labels = {
            bet["id"]: bet["track"] or "Unknown"
            for bet in enriched_all_bets
        }
        race_type_labels = {
            bet["id"]: bet["race_type"] or bet["meeting_type"] or "Unknown"
            for bet in enriched_all_bets
        }
        track_stats = build_label_breakdown(settled_bets, track_labels, limit=8)
        race_type_stats = build_label_breakdown(settled_bets, race_type_labels, limit=8)
        calibration = summarize_calibration(collect_calibration_rows(db))

        return render_template_string(
            HTML_TEMPLATE,
            active_version=ACTIVE_DECISION_VERSION,
            starting_bank=f"{STARTING_BANK:.2f}",
            current_bank=f"{current_bank:.2f}",
            lifetime_bank=f"{lifetime_bank:.2f}",
            bank_since_reset=f"{bank_since_reset:.2f}",
            bank_since_reset_value=bank_since_reset,
            reset_baseline=f"{reset_baseline:.2f}",
            reset_date=reset_date,
            total_roi=f"{total_roi:.2%}",
            bets_today=bets_today,
            open_bets=open_bets,
            open_exposure=f"{status_breakdown['open']['stake_exposure']:.2f}",
            daily_pl=f"{daily_pl:.2f}",
            daily_pl_value=daily_pl,
            version_stats=version_stats,
            odds_bucket_stats=odds_bucket_stats,
            status_breakdown=status_breakdown,
            track_stats=track_stats,
            race_type_stats=race_type_stats,
            recent_open_bets=recent_open_bets,
            recent_settled_bets=recent_settled_bets,
            bank_chart_labels=bank_chart_labels,
            bank_chart_values=bank_chart_values,
            calibration_brier=(
                f"{calibration['brier_score']:.4f}"
                if calibration["brier_score"] is not None
                else "N/A"
            ),
            calibration_rows=len(calibration["rows"]),
            calibration_bucket_count=len(calibration["bucket_summaries"]),
            calibration_buckets=calibration["bucket_summaries"][:6],
        )
    finally:
        db.close()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=False)
