"""Transport bullets must lead with a scannable mode prefix even when the
LLM rewrite drops it (regression for 2026-06-13: lines opened with bare
"Остановки …" / "Железнодорожные услуги …")."""

from news_digest.pipeline.writer import _ensure_transport_mode_prefix


def test_transport_mode_prefix_is_restored() -> None:
    # Mode from candidate is authoritative.
    assert _ensure_transport_mode_prefix(
        "• Остановки на Oxford Road закрыты до 20 июня.", {"transport_mode": "bus"}
    ).startswith("• Автобусы: ")
    # No candidate → infer rail from text.
    assert _ensure_transport_mode_prefix(
        "• Железнодорожные услуги между Piccadilly и Stockport приостановлены.", None
    ).startswith("• National Rail: ")
    # Already prefixed → untouched.
    metrolink = "• Metrolink: с 14 по 16 июня нет трамваев на Bury line."
    assert _ensure_transport_mode_prefix(metrolink, {"transport_mode": "tram"}) == metrolink
    # Unclassifiable → left as-is rather than mislabelled.
    vague = "• Что-то непонятное произошло сегодня."
    assert _ensure_transport_mode_prefix(vague, None) == vague
