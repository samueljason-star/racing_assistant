# Server Setup

## Clone And Setup

```bash
git clone https://github.com/samueljason-star/racing_assistant.git racing_assistant
cd racing_assistant
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Environment Variables

Create a `.env` file in the project root with:

```env
DATABASE_URL=sqlite:///racing_assistant.db

BETFAIR_APP_KEY=your_app_key_here
BETFAIR_SSID=your_betfair_session_token
BETFAIR_USERNAME=your_username
BETFAIR_PASSWORD=your_password

TELEGRAM_BOT_TOKEN=your_telegram_bot_token
TELEGRAM_CHAT_ID=your_telegram_chat_id

BETFAIR_COMMISSION_RATE=0.08
PAPER_STAKE_PCT=0.01
PAPER_BANK_BASELINE=10000
PAPER_MIN_ODDS=3.0
PAPER_MAX_ODDS=20.0
PAPER_MIN_EDGE=0.05
ACTIVE_DECISION_VERSION=model_edge_v2
```

## Apply The V2 Upgrade

Run these commands after pulling the upgraded code:

```bash
cd /home/samueljasonhines/racing_assistant
source venv/bin/activate
python3 -c "from app.db import init_db; init_db()"
python3 -m app.reports.calibration_report
python3 -m app.reports.daily_summary
```

## Reset The Paper Bank

Archive current paper bets and start a fresh `$10,000` cycle:

```bash
cd /home/samueljasonhines/racing_assistant
source venv/bin/activate
python3 -m app.betting.reset_paper_bank --baseline 10000 --note "model_edge_v2 reset"
```

Delete current paper bets instead of archiving them:

```bash
python3 -m app.betting.reset_paper_bank --delete-existing --baseline 10000 --note "model_edge_v2 reset"
```

## Services

Scheduler service:

```bash
sudo cp deploy/racing-assistant-scheduler.service /etc/systemd/system/
```

Dashboard service:

```bash
sudo cp deploy/racing-assistant-dashboard.service /etc/systemd/system/
```

Reload and restart:

```bash
sudo systemctl daemon-reload
sudo systemctl enable racing-assistant-scheduler racing-assistant-dashboard
sudo systemctl restart racing-assistant-scheduler racing-assistant-dashboard
sudo systemctl status racing-assistant-scheduler racing-assistant-dashboard --no-pager
```

## Run Manually

Dashboard:

```bash
cd /home/samueljasonhines/racing_assistant
source venv/bin/activate
python3 -m app.dashboard.run_dashboard
```

Live scheduler:

```bash
cd /home/samueljasonhines/racing_assistant
source venv/bin/activate
python3 app/scheduler/run_live_scheduler.py
```

## Access From Your Phone

Open:

```text
http://YOUR_SERVER_IP:8000
```

Example:

```text
http://192.168.1.50:8000
```
