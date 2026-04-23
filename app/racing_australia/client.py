from __future__ import annotations

import requests

BASE_URL = "https://www.racingaustralia.horse"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


def build_url(path: str) -> str:
    if path.startswith("http"):
        return path
    if not path.startswith("/"):
        path = f"/{path}"
    return f"{BASE_URL}{path}"


def get_html(url: str, timeout: int = 30) -> str:
    response = requests.get(
        build_url(url),
        headers={
            "User-Agent": USER_AGENT,
            "Accept-Language": "en-AU,en;q=0.9",
        },
        timeout=timeout,
    )
    response.raise_for_status()
    return response.text
