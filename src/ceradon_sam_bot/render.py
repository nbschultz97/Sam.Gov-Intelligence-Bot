from __future__ import annotations

from typing import Iterable


def render_digest(rows: Iterable[object]) -> str:
    lines = ["Ceradon SAM Opportunity Digest", ""]
    count = 0
    for row in rows:
        count += 1
        lines.extend(
            [
                f"{count}. {row['title']}",
                f"   Agency: {row['agency']}",
                f"   Notice Type: {row['notice_type']}",
                f"   NAICS: {row['naics']}",
                f"   Set-Aside: {row['set_aside'] or 'N/A'}",
                f"   Posted: {row['posted_date']}",
                f"   Deadline: {row['response_deadline']}",
                f"   Score: {row['score']}",
                f"   Link: {row['link']}",
                "",
            ]
        )
    if count == 0:
        lines.append("No opportunities met the digest threshold.")
    return "\n".join(lines)
