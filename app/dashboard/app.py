import sys
from pathlib import Path

from flask import Flask, render_template_string

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.betting.bet_details import enrich_paper_bets
from app.betting.paper_bank import STARTING_BANK, get_current_bank, get_total_roi
from app.db import SessionLocal
from app.models import PaperBet

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
    body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: #f5f7fb;
      color: #1f2937;
      margin: 0;
      padding: 16px;
    }
    .container {
      max-width: 720px;
      margin: 0 auto;
    }
    h1 {
      font-size: 1.5rem;
      margin-bottom: 16px;
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 16px;
    }
    .card {
      background: white;
      border-radius: 14px;
      padding: 14px;
      box-shadow: 0 2px 10px rgba(15, 23, 42, 0.08);
    }
    .card h2 {
      font-size: 0.85rem;
      margin: 0 0 8px;
      color: #6b7280;
      font-weight: 600;
    }
    .value {
      font-size: 1.3rem;
      font-weight: 700;
    }
    .section {
      margin-top: 16px;
    }
    .section h3 {
      font-size: 1rem;
      margin-bottom: 10px;
    }
    .bet-list {
      display: grid;
      gap: 10px;
    }
    .bet-item {
      background: white;
      border-radius: 14px;
      padding: 12px;
      box-shadow: 0 2px 10px rgba(15, 23, 42, 0.08);
    }
    .bet-title {
      font-weight: 700;
      margin-bottom: 6px;
    }
    .muted {
      color: #6b7280;
      font-size: 0.9rem;
    }
    canvas {
      background: white;
      border-radius: 14px;
      padding: 10px;
      box-shadow: 0 2px 10px rgba(15, 23, 42, 0.08);
    }
    @media (max-width: 520px) {
      .grid {
        grid-template-columns: 1fr 1fr;
      }
      .value {
        font-size: 1.1rem;
      }
    }
  </style>
</head>
<body>
  <div class="container">
    <h1>Racing Assistant</h1>

    <div class="grid">
      <div class="card"><h2>Current Bank</h2><div class="value">${{ current_bank }}</div></div>
      <div class="card"><h2>Daily P/L</h2><div class="value">${{ daily_pl }}</div></div>
      <div class="card"><h2>Total ROI</h2><div class="value">{{ total_roi }}</div></div>
      <div class="card"><h2>Bets Today</h2><div class="value">{{ bets_today }}</div></div>
      <div class="card"><h2>Open Bets</h2><div class="value">{{ open_bets }}</div></div>
      <div class="card"><h2>Starting Bank</h2><div class="value">${{ starting_bank }}</div></div>
    </div>

    <div class="section">
      <h3>Bank Over Time</h3>
      <canvas id="bankChart"></canvas>
    </div>

    <div class="section">
      <h3>Recent Paper Bets</h3>
      <div class="bet-list">
        {% for bet in recent_paper_bets %}
        <div class="bet-item">
          <div class="bet-title">{{ bet.horse_name }} | {{ bet.track or "Unknown" }} R{{ bet.race_number or "?" }}</div>
          <div class="muted">Odds: {{ "%.2f"|format(bet.odds_taken) }} | Stake: ${{ "%.2f"|format(bet.stake) }} | Edge: {{ "%.4f"|format(bet.edge) }}</div>
          <div class="muted">Race ID: {{ bet.race_id }}</div>
          <div class="muted">Market: {{ "%.4f"|format(bet.market_probability) }} | Model: {{ "%.4f"|format(bet.model_probability) }}</div>
          <div class="muted">Reason: {{ bet.decision_reason or "N/A" }}</div>
          <div class="muted">Version: {{ bet.decision_version }} | Result: {{ bet.result or "OPEN" }} | P/L: ${{ "%.2f"|format(bet.profit_loss or 0) }}</div>
        </div>
        {% else %}
        <div class="muted">No paper bets found.</div>
        {% endfor %}
      </div>
    </div>

    <div class="section">
      <h3>Recent Settled Bets</h3>
      <div class="bet-list">
        {% for bet in recent_settled_bets %}
        <div class="bet-item">
          <div class="bet-title">{{ bet.horse_name }} | {{ bet.track or "Unknown" }} R{{ bet.race_number or "?" }} | {{ bet.result or "N/A" }}</div>
          <div class="muted">Odds: {{ "%.2f"|format(bet.odds_taken) }} | Stake: ${{ "%.2f"|format(bet.stake) }} | P/L: ${{ "%.2f"|format(bet.profit_loss or 0) }}</div>
          <div class="muted">Race ID: {{ bet.race_id }}</div>
          <div class="muted">Market: {{ "%.4f"|format(bet.market_probability) }} | Model: {{ "%.4f"|format(bet.model_probability) }} | Edge: {{ "%.4f"|format(bet.edge) }}</div>
          <div class="muted">Reason: {{ bet.decision_reason or "N/A" }}</div>
          <div class="muted">Version: {{ bet.decision_version }}</div>
        </div>
        {% else %}
        <div class="muted">No settled bets found.</div>
        {% endfor %}
      </div>
    </div>
  </div>

  <script>
    const labels = {{ bank_chart_labels|tojson }};
    const values = {{ bank_chart_values|tojson }};

    new Chart(document.getElementById('bankChart'), {
      type: 'line',
      data: {
        labels,
        datasets: [{
          label: 'Bank',
          data: values,
          borderColor: '#0f766e',
          backgroundColor: 'rgba(15, 118, 110, 0.12)',
          fill: true,
          tension: 0.25
        }]
      },
      options: {
        responsive: true,
        plugins: { legend: { display: false } },
        scales: {
          y: { beginAtZero: false }
        }
      }
    });
  </script>
</body>
</html>
"""


def _bank_series(settled_bets):
    labels = ["Start"]
    values = [STARTING_BANK]
    running_bank = STARTING_BANK

    for index, bet in enumerate(settled_bets, start=1):
        running_bank += bet.profit_loss or 0.0
        labels.append(f"Bet {index}")
        values.append(round(running_bank, 2))

    return labels, values


@app.route("/")
def index():
    db = SessionLocal()

    try:
        all_bets = db.query(PaperBet).order_by(PaperBet.id.desc()).all()
        settled_bets = [bet for bet in reversed(all_bets) if bet.settled_flag and bet.profit_loss is not None]
        recent_paper_bets = enrich_paper_bets(db, all_bets[:20])
        recent_settled_bets = enrich_paper_bets(
            db,
            [bet for bet in all_bets if bet.settled_flag][:20],
        )

        current_bank = get_current_bank(db)
        total_roi = get_total_roi(db)
        daily_pl = round(sum((bet["profit_loss"] or 0.0) for bet in recent_settled_bets), 2)
        bets_today = len(recent_paper_bets)
        open_bets = sum(1 for bet in all_bets if not bet.settled_flag)
        bank_chart_labels, bank_chart_values = _bank_series(settled_bets)

        return render_template_string(
            HTML_TEMPLATE,
            starting_bank=f"{STARTING_BANK:.2f}",
            current_bank=f"{current_bank:.2f}",
            daily_pl=f"{daily_pl:.2f}",
            total_roi=f"{total_roi:.2%}",
            bets_today=bets_today,
            open_bets=open_bets,
            recent_paper_bets=recent_paper_bets,
            recent_settled_bets=recent_settled_bets,
            bank_chart_labels=bank_chart_labels,
            bank_chart_values=bank_chart_values,
        )
    finally:
        db.close()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
