from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests

from app.config import PUNTING_FORM_API_KEY

BASE_URL = "https://api.puntingform.com.au/v2/form"


@dataclass(frozen=True)
class EndpointSpec:
    name: str
    path: str
    format: str
    entity_type: str | None = None


VISIBLE_FORM_ENDPOINTS = (
    EndpointSpec("fields", "fields", "json"),
    EndpointSpec("fields_csv", "fields/csv", "csv"),
    EndpointSpec("form", "form", "json"),
    EndpointSpec("form_csv", "form/csv", "csv"),
    EndpointSpec("meetings_list", "meetingslist", "json"),
    EndpointSpec("meetings_list_csv", "meetingslist/csv", "csv"),
    EndpointSpec("meeting", "meeting", "json"),
    EndpointSpec("meeting_csv", "meeting/csv", "csv"),
    EndpointSpec("results", "results", "json"),
    EndpointSpec("results_csv", "results/csv", "csv"),
    EndpointSpec("strike_rate_trainer", "strikerate", "json", entity_type="trainer"),
    EndpointSpec(
        "strike_rate_trainer_csv",
        "strikerate/csv",
        "csv",
        entity_type="trainer",
    ),
    EndpointSpec("strike_rate_jockey", "strikerate", "json", entity_type="jockey"),
    EndpointSpec(
        "strike_rate_jockey_csv",
        "strikerate/csv",
        "csv",
        entity_type="jockey",
    ),
)

DATE_PARAM_CANDIDATES = ("date", "meetingDate", "fromDate")


def require_api_key(api_key: str | None = None) -> str:
    resolved = api_key or PUNTING_FORM_API_KEY
    if resolved:
        return resolved
    raise RuntimeError(
        "PUNTING_FORM_API_KEY is not set. Add it to .env before calling Punting Form."
    )


def build_headers(expect_csv: bool = False) -> dict[str, str]:
    return {
        "accept": "text/csv" if expect_csv else "application/json",
        "user-agent": "racing-assistant/1.0",
    }


def build_meetings_list_params(api_key: str, date_value: str, param_name: str) -> dict[str, Any]:
    params: dict[str, Any] = {
        "apiKey": api_key,
        param_name: date_value,
    }
    if param_name == "fromDate":
        params["toDate"] = date_value
    return params


def build_meeting_params(
    api_key: str,
    meeting_id: int,
    *,
    race_number: int = 0,
    runs: int = 10,
    entity_type: str | None = None,
) -> dict[str, Any]:
    params = {
        "apiKey": api_key,
        "meetingId": meeting_id,
        "raceNumber": race_number,
        "runs": runs,
    }
    if entity_type:
        params["entityType"] = entity_type
    return params


def request_endpoint(
    endpoint: EndpointSpec,
    params: dict[str, Any],
    *,
    timeout: int = 60,
) -> requests.Response:
    response = requests.get(
        f"{BASE_URL}/{endpoint.path}",
        headers=build_headers(expect_csv=endpoint.format == "csv"),
        params=params,
        timeout=timeout,
    )
    response.raise_for_status()
    return response
