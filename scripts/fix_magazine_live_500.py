"""Move magazine 3POINT generation after its inputs are initialized.

This fixes the production trend-detail 500 introduced by magazine-mode v1.
No database or network changes are made.
"""
from pathlib import Path
import ast

ROOT = Path(__file__).resolve().parents[1]
MAIN = ROOT / "app/main.py"
CALL = "    buzz_points = magazine.three_points(trend, briefing, editorial_brief)\n"
BRIEFING = "    briefing = _article_briefing(sources, trend[\"keyword\"])\n"


def apply():
    text = MAIN.read_text()
    if CALL not in text:
        raise ValueError("magazine three_points call not found")
    if BRIEFING not in text:
        raise ValueError("article briefing initialization not found")

    call_pos = text.index(CALL)
    briefing_pos = text.index(BRIEFING)
    if call_pos < briefing_pos:
        text = text.replace(CALL, "", 1)
        text = text.replace(BRIEFING, BRIEFING + CALL, 1)
        # Removing an indented statement can leave an indented blank line.
        text = text.replace("\n    \n", "\n\n")
    elif call_pos == briefing_pos:
        raise ValueError("unexpected overlapping anchors")

    # Safety: both inputs must be initialized before three_points is evaluated.
    assert text.index("    editorial_brief = editorial.load_brief(db, trend[\"id\"])\n") < text.index(CALL)
    assert text.index(BRIEFING) < text.index(CALL)
    ast.parse(text)
    MAIN.write_text(text)


if __name__ == "__main__":
    apply()
    print("Fixed magazine detail ordering: editorial/briefing -> 3POINT")
