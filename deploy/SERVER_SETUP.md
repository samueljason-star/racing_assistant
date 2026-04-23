# Server Setup

## Clone And Setup

```bash
git clone YOUR_REPO_URL racing_assistant
cd racing_assistant
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Flask Dashboard

Start the dashboard manually on the server:

```bash
cd /Users/sam/Desktop/racing_assistant
FLASK_APP=app/dashboard/app.py flask run --host=0.0.0.0 --port=8000
```

Or run it directly with Python:

```bash
cd /Users/sam/Desktop/racing_assistant
python3 app/dashboard/app.py
```

If you use the direct Python command, update the app to listen on port `8000` or run it behind a port forward/proxy. The easiest option is the Flask command above.

## Access From Your Phone

Make sure your phone is on the same network as the server, then open:

```text
http://YOUR_SERVER_IP:8000
```

Example:

```text
http://192.168.1.50:8000
```
