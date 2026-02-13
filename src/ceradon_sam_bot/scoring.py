from __future__ import annotations

import datetime as dt
from typing import Any, Dict, List, Tuple

from ceradon_sam_bot.config import Config


def _parse_date(value: str) -> dt.date | None:
    if not value:
        return None
    try:
        cleaned = value.replace("Z", "+00:00")
        parsed = dt.datetime.fromisoformat(cleaned)
        return parsed.date()
    except ValueError:
        try:
            return dt.datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError:
            return None


def score_opportunity(opportunity: Dict[str, Any], config: Config) -> Tuple[int, List[str]]:
    text_blob = " ".join(
        [
            str(opportunity.get("title", "")),
            str(opportunity.get("description", "")),
            str(opportunity.get("agency", "")),
        ]
    ).lower()

    score = 0
    reasons: List[str] = []

    for keyword, weight in config.keywords.positive.items():
        if keyword in text_blob:
            score += weight
            reasons.append(f"+{weight} keyword:{keyword}")

    for keyword, weight in config.keywords.negative.items():
        if keyword in text_blob:
            score -= weight
            reasons.append(f"-{weight} keyword:{keyword}")

    naics = str(opportunity.get("naics", ""))
    if naics and naics in config.filters.naics_include:
        score += config.scoring.naics_match_boost
        reasons.append(f"+{config.scoring.naics_match_boost} naics:{naics}")

    notice_type = str(opportunity.get("notice_type", ""))
    if notice_type in config.filters.preferred_notice_types:
        score += config.scoring.notice_type_boost
        reasons.append(f"+{config.scoring.notice_type_boost} notice_type:{notice_type}")

    set_aside = str(opportunity.get("set_aside", "")).lower()
    set_aside_desc = str(opportunity.get("set_aside_description", "")).lower()
    set_aside_combined = f"{set_aside} {set_aside_desc}"
    if "sdvosb" in set_aside_combined or "service-disabled" in set_aside_combined:
        # Extra boost for SDVOSB (Ceradon's status)
        score += config.scoring.set_aside_boost
        reasons.append(f"+{config.scoring.set_aside_boost} sdvosb_match")
    if "small business" in set_aside_combined or "sb" == set_aside:
        score += config.scoring.set_aside_boost
        reasons.append(f"+{config.scoring.set_aside_boost} set_aside:{set_aside}")

    deadline = _parse_date(str(opportunity.get("response_deadline", "")))
    if deadline:
        days_until = (deadline - dt.date.today()).days
        if days_until <= 7:
            score += config.scoring.deadline_urgency_boost
            reasons.append(
                f"+{config.scoring.deadline_urgency_boost} deadline_in:{days_until}d"
            )

    return score, reasons
