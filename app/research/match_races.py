from __future__ import annotations

from pathlib import Path

import pandas as pd

from app.research.utils import (
    RESEARCH_DATA_DIR,
    attach_common_labels,
    derive_price_movement_features,
    normalize_track_name,
    parse_float,
    parse_int,
    parse_price,
    save_dataframe,
)

PUNTING_FORM_PATH = RESEARCH_DATA_DIR / "punting_form_clean.csv"
BETFAIR_PATH = RESEARCH_DATA_DIR / "betfair_odds_clean.csv"
MATCHED_OUTPUT_PATH = RESEARCH_DATA_DIR / "matched_runner_data.csv"
UNMATCHED_PF_PATH = RESEARCH_DATA_DIR / "unmatched_punting_form.csv"
UNMATCHED_BF_PATH = RESEARCH_DATA_DIR / "unmatched_betfair.csv"


def _prepare_frame(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    frame = pd.read_csv(path, low_memory=False)
    return attach_common_labels(frame)


def _first_valid(series: pd.Series) -> float | None:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return None
    return round(float(numeric.iloc[0]), 4)


def _closest_price(group: pd.DataFrame, target_minutes: int) -> float | None:
    if "minutes_to_jump" not in group.columns:
        return None
    working = group.copy()
    working["minutes_to_jump"] = pd.to_numeric(working["minutes_to_jump"], errors="coerce")
    working = working.dropna(subset=["minutes_to_jump"])
    if working.empty:
        return None
    working["distance_to_target"] = (working["minutes_to_jump"] - target_minutes).abs()
    working = working.sort_values(["distance_to_target", "timestamp"])
    candidate = working.iloc[0]
    traded_price = parse_price(candidate.get("traded_price"))
    best_back_price = parse_price(candidate.get("best_back_price"))
    return traded_price if traded_price is not None else best_back_price


def _summarize_betfair_group(group: pd.DataFrame) -> dict[str, object]:
    ordered = group.sort_values("timestamp")
    opening_price = _first_valid(
        ordered["traded_price"].combine_first(ordered["best_back_price"])
    )
    closing_price = _first_valid(
        ordered.iloc[::-1]["traded_price"].combine_first(ordered.iloc[::-1]["best_back_price"])
    )
    return {
        "market_id": ordered["market_id"].dropna().astype(str).iloc[0] if ordered["market_id"].notna().any() else None,
        "selection_id": ordered["selection_id"].dropna().astype(str).iloc[0] if ordered["selection_id"].notna().any() else None,
        "opening_price": opening_price,
        "price_60m": _closest_price(ordered, 60),
        "price_30m": _closest_price(ordered, 30),
        "price_10m": _closest_price(ordered, 10),
        "price_5m": _closest_price(ordered, 5),
        "closing_price": closing_price,
        "best_back_price": _first_valid(ordered.iloc[::-1]["best_back_price"]),
        "best_lay_price": _first_valid(ordered.iloc[::-1]["best_lay_price"]),
        "total_matched": _first_valid(ordered.iloc[::-1]["total_matched"]),
    }


def _match_key(row: pd.Series, include_race_number: bool = True) -> tuple:
    key = (
        row.get("race_date"),
        normalize_track_name(row.get("track")),
        row.get("horse_name_norm"),
    )
    if include_race_number:
        return key + (parse_int(row.get("race_number")),)
    return key


def match_races(
    punting_form_path: Path = PUNTING_FORM_PATH,
    betfair_path: Path = BETFAIR_PATH,
) -> pd.DataFrame:
    punting_form = _prepare_frame(punting_form_path)
    betfair = _prepare_frame(betfair_path)

    if punting_form.empty:
        save_dataframe(pd.DataFrame(), MATCHED_OUTPUT_PATH)
        save_dataframe(pd.DataFrame(), UNMATCHED_PF_PATH)
        save_dataframe(pd.DataFrame(), UNMATCHED_BF_PATH)
        print("Race Matching Summary")
        print("TOTAL PUNTING FORM ROWS: 0")
        print("TOTAL MATCHED ROWS: 0")
        print("UNMATCHED HORSES: 0")
        print("UNMATCHED RACES: 0")
        print("MATCH RATE: 0.00%")
        print("WARNING: No cleaned Punting Form rows were available to match.")
        return pd.DataFrame()

    if betfair.empty:
        save_dataframe(pd.DataFrame(columns=punting_form.columns), MATCHED_OUTPUT_PATH)
        save_dataframe(punting_form, UNMATCHED_PF_PATH)
        save_dataframe(pd.DataFrame(), UNMATCHED_BF_PATH)
        total_races = set(
            zip(
                punting_form.get("race_date", pd.Series(dtype=object)),
                punting_form.get("track_norm", pd.Series(dtype=object)),
                punting_form.get("race_number", pd.Series(dtype=object)),
            )
        )
        print("Race Matching Summary")
        print(f"TOTAL PUNTING FORM ROWS: {len(punting_form)}")
        print("TOTAL MATCHED ROWS: 0")
        print(f"UNMATCHED HORSES: {len(punting_form)}")
        print(f"UNMATCHED RACES: {len(total_races)}")
        print("MATCH RATE: 0.00%")
        print("WARNING: No cleaned Betfair history rows were available to match.")
        return pd.DataFrame(columns=punting_form.columns)

    betfair_groups: dict[tuple, pd.DataFrame] = {}
    betfair_groups_no_race: dict[tuple, pd.DataFrame] = {}
    for key, group in betfair.groupby(
        ["race_date", "track_norm", "horse_name_norm", "race_number"], dropna=False
    ):
        betfair_groups[key] = group
    for key, group in betfair.groupby(
        ["race_date", "track_norm", "horse_name_norm"], dropna=False
    ):
        betfair_groups_no_race[key] = group

    matched_rows: list[dict[str, object]] = []
    matched_betfair_indexes: set[int] = set()
    unmatched_punting_rows: list[dict[str, object]] = []

    for _, row in punting_form.iterrows():
        key_with_race = _match_key(row, include_race_number=True)
        key_without_race = _match_key(row, include_race_number=False)
        group = betfair_groups.get(key_with_race)
        if group is None:
            group = betfair_groups_no_race.get(key_without_race)
        if group is None or group.empty:
            unmatched_punting_rows.append(row.to_dict())
            continue

        matched_betfair_indexes.update(group.index.tolist())
        odds_summary = _summarize_betfair_group(group)
        matched_rows.append({**row.to_dict(), **odds_summary})

    matched_frame = pd.DataFrame(matched_rows)
    unmatched_betfair = betfair.loc[~betfair.index.isin(matched_betfair_indexes)].copy()

    if not matched_frame.empty:
        matched_frame = derive_price_movement_features(matched_frame)
        matched_frame["won_flag"] = matched_frame["finish_position"].map(
            lambda value: 1 if parse_int(value) == 1 else 0
        )
    else:
        matched_frame = pd.DataFrame()

    save_dataframe(matched_frame, MATCHED_OUTPUT_PATH)
    save_dataframe(pd.DataFrame(unmatched_punting_rows), UNMATCHED_PF_PATH)
    save_dataframe(unmatched_betfair, UNMATCHED_BF_PATH)

    total_pf_rows = len(punting_form)
    total_matched_rows = len(matched_frame)
    match_rate = round(total_matched_rows / total_pf_rows, 4) if total_pf_rows else 0.0

    matched_races = set(
        zip(
            matched_frame.get("race_date", pd.Series(dtype=object)),
            matched_frame.get("track_norm", pd.Series(dtype=object)),
            matched_frame.get("race_number", pd.Series(dtype=object)),
        )
    )
    total_races = set(
        zip(
            punting_form.get("race_date", pd.Series(dtype=object)),
            punting_form.get("track_norm", pd.Series(dtype=object)),
            punting_form.get("race_number", pd.Series(dtype=object)),
        )
    )
    unmatched_races = max(len(total_races - matched_races), 0)

    print("Race Matching Summary")
    print(f"TOTAL PUNTING FORM ROWS: {total_pf_rows}")
    print(f"TOTAL MATCHED ROWS: {total_matched_rows}")
    print(f"UNMATCHED HORSES: {len(unmatched_punting_rows)}")
    print(f"UNMATCHED RACES: {unmatched_races}")
    print(f"MATCH RATE: {match_rate:.2%}")
    return matched_frame


def main() -> None:
    match_races()


if __name__ == "__main__":
    main()
