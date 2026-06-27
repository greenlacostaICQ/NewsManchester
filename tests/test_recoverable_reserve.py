"""Wave 1 / S1: unify the two reserve pools so a strong news item cut purely
for board capacity is reachable by the editor backfill.

Before S1 the board overflow was tagged ``public_reserve=False`` +
``backup_pool_only=True``, which the backfill (``_same_section_reserve_line``)
explicitly skipped. That disjoint pool is why every prior backfill fix failed to
refill thin news blocks — the strong cut news (e.g. the mayoral explainer, the
Cheetham Hill explosion) sat in a pool the backfill could not read.
"""
from news_digest.pipeline import editor
from news_digest.pipeline.common import is_recoverable_reserve, recoverable_reserve_eligible


def _capacity_cut(**over):
    candidate = {
        "validated": True,
        "digest_selection_verdict": "reserve",
        "primary_block": "last_24h",
        "draft_line": (
            '• В Манчестере открыли новый общественный парк для жителей района. '
            '<a href="https://example.test/park">MEN</a>'
        ),
        # how the board cut historically tagged the overflow:
        "backup_candidate": True,
        "backup_pool_only": True,
        "public_reserve": False,
        "recoverable_reserve": True,
    }
    candidate.update(over)
    return candidate


def test_capacity_cut_overflow_is_recoverable():
    assert recoverable_reserve_eligible(_capacity_cut()) is True
    # The fix: backup_pool_only no longer hides a capacity-cut item from backfill.
    assert is_recoverable_reserve(_capacity_cut()) is True


def test_excluded_categories_never_recoverable():
    # Owner rule: never re-admit quarantine / manual-review / rejected / stale /
    # duplicate / low-trust material into the public issue.
    for bad in (
        {"validated": False},
        {"synthetic_stale": True},
        {"source_trial": True},
        {"digest_selection_verdict": "drop"},
        {"publish_plan_status": "drop"},
        {"reject_reasons": ["no_date"]},
        {"dedupe_decision": "duplicate"},
        {"held_for_manual_review": True},
    ):
        assert recoverable_reserve_eligible(_capacity_cut(**bad)) is False, bad


def test_editor_backfill_now_reaches_capacity_cut_reserve():
    # Integration: the same candidate the old reader skipped is now returned.
    line = editor._same_section_reserve_line("Свежие новости", [_capacity_cut()], set(), set())
    assert line.startswith("• ")
    assert "парк" in line
