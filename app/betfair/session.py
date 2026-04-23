from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode

import requests

from app.config import (
    BETFAIR_APP_KEY,
    BETFAIR_CERT_FILE,
    BETFAIR_IDENTITY_BASE_URL,
    BETFAIR_KEEPALIVE_URL,
    BETFAIR_KEY_FILE,
    BETFAIR_PASSWORD,
    BETFAIR_SSID,
    BETFAIR_USERNAME,
)

BETFAIR_API_URL = "https://api.betfair.com/exchange/betting/json-rpc/v1"
CERT_LOGIN_URL = f"{BETFAIR_IDENTITY_BASE_URL.rstrip('/')}/api/certlogin"
SESSION_REFRESH_INTERVAL = timedelta(hours=6)

_session_token: str | None = None
_session_source: str | None = None
_session_valid_until: datetime | None = None


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _has_certificate_config() -> bool:
    return bool(BETFAIR_CERT_FILE and BETFAIR_KEY_FILE and BETFAIR_USERNAME and BETFAIR_PASSWORD)


def _cert_paths() -> tuple[str, str]:
    cert_file = Path(BETFAIR_CERT_FILE or "")
    key_file = Path(BETFAIR_KEY_FILE or "")
    if not cert_file.exists():
        raise FileNotFoundError(f"Betfair cert file not found: {cert_file}")
    if not key_file.exists():
        raise FileNotFoundError(f"Betfair key file not found: {key_file}")
    return str(cert_file), str(key_file)


def _set_session_token(token: str, source: str) -> str:
    global _session_token, _session_source, _session_valid_until
    _session_token = token
    _session_source = source
    _session_valid_until = _utc_now() + SESSION_REFRESH_INTERVAL
    return token


def clear_session_token() -> None:
    global _session_token, _session_source, _session_valid_until
    _session_token = None
    _session_source = None
    _session_valid_until = None


def _cert_login() -> str:
    if not BETFAIR_APP_KEY:
        raise ValueError("Missing BETFAIR_APP_KEY in .env.")
    if not _has_certificate_config():
        raise ValueError(
            "Missing Betfair certificate configuration. "
            "Set BETFAIR_CERT_FILE, BETFAIR_KEY_FILE, BETFAIR_USERNAME, and BETFAIR_PASSWORD."
        )

    cert, key = _cert_paths()
    response = requests.post(
        CERT_LOGIN_URL,
        headers={
            "X-Application": BETFAIR_APP_KEY,
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data=urlencode(
            {
                "username": BETFAIR_USERNAME or "",
                "password": BETFAIR_PASSWORD or "",
            }
        ),
        cert=(cert, key),
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()
    if data.get("loginStatus") != "SUCCESS" or not data.get("sessionToken"):
        raise RuntimeError(f"Betfair cert login failed: {data}")
    return _set_session_token(data["sessionToken"], "certlogin")


def keep_alive() -> bool:
    token = _session_token or BETFAIR_SSID
    if not BETFAIR_APP_KEY or not token:
        return False

    response = requests.post(
        BETFAIR_KEEPALIVE_URL,
        headers={
            "X-Application": BETFAIR_APP_KEY,
            "X-Authentication": token,
            "Accept": "application/json",
        },
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()
    if data.get("status") != "SUCCESS":
        return False

    _set_session_token(token, _session_source or ("env_ssid" if BETFAIR_SSID else "keepalive"))
    return True


def get_session_token(force_refresh: bool = False) -> str:
    token = _session_token
    if (
        token
        and not force_refresh
        and _session_valid_until is not None
        and _session_valid_until > _utc_now()
    ):
        return token

    if token and not force_refresh and keep_alive():
        return _session_token or token

    if _has_certificate_config():
        return _cert_login()

    if BETFAIR_SSID:
        return _set_session_token(BETFAIR_SSID, "env_ssid")

    raise ValueError(
        "No Betfair session available. Provide cert login config or BETFAIR_SSID."
    )


def build_api_headers(force_refresh: bool = False) -> dict[str, str]:
    if not BETFAIR_APP_KEY:
        raise ValueError(
            "Missing Betfair app key. Set BETFAIR_APP_KEY in .env "
            "(APP_KEY is still accepted as a fallback)."
        )

    return {
        "X-Application": BETFAIR_APP_KEY,
        "X-Authentication": get_session_token(force_refresh=force_refresh),
        "Content-Type": "application/json",
    }


def post_json_rpc(payload, timeout: int = 30):
    response = requests.post(
        BETFAIR_API_URL,
        data=json.dumps(payload),
        headers=build_api_headers(),
        timeout=timeout,
    )
    response.raise_for_status()
    data = response.json()

    if _contains_invalid_session_error(data):
        clear_session_token()
        response = requests.post(
            BETFAIR_API_URL,
            data=json.dumps(payload),
            headers=build_api_headers(force_refresh=True),
            timeout=timeout,
        )
        response.raise_for_status()
        data = response.json()

    return data


def _contains_invalid_session_error(data) -> bool:
    if not isinstance(data, list) or not data:
        return False
    error = data[0].get("error", {})
    details = error.get("data", {}).get("APINGException", {})
    return details.get("errorCode") == "INVALID_SESSION_INFORMATION"
