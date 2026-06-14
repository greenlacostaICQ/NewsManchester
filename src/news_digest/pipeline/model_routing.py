"""Central model routing policy for LLM-backed pipeline decisions.

The policy keeps cheap classification/reject tasks separate from higher
stakes rewrite tasks. Environment overrides still work for local debugging,
but the default route is explicit and reportable.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import os

DEEPSEEK_BASE_URL = "https://api.deepseek.com/v1"
DEEPSEEK_MODEL = "deepseek-v4-flash"
DEEPSEEK_PRO_MODEL = "deepseek-v4-pro"
OPENAI_BASE_URL = "https://api.openai.com/v1"
# Prose rewrite (the editorial Russian cards) runs on gpt-4o: gpt-4o-mini was
# unreliable on rich evidence — on 2026-06-05 it returned empty on 919/915-char
# stories plus 14 malformed outputs, forcing the DeepSeek fallback daily. gpt-4o
# is the right-sized model for quality-critical generation (~$13.5/mo on this
# volume). Classification/dedupe scoring stays on mini — it doesn't need 4o and
# that's where cost would balloon.
OPENAI_REWRITE_MODEL = "gpt-4o"
OPENAI_SCORING_MODEL = "gpt-4o-mini"
# Transport reverted to gpt-4o: on 2026-06-13 mini returned EMPTY on two tram
# lift alerts that had 330/427 chars of evidence (Dane Road, Queens Road), so
# they silently dropped — gpt-4o wrote them in earlier runs. Reliability on the
# Metrolink-critical block beats the few-cents/day saving. (Route kept separate
# only for its larger batch / tighter timeout — short transport lines.)
OPENAI_TRANSPORT_MODEL = "gpt-4o"
GROQ_BASE_URL = "https://api.groq.com/openai/v1"
GROQ_FALLBACK_MODEL = "llama-3.3-70b-versatile"


@dataclass(frozen=True, slots=True)
class ModelRouteStep:
    provider: str
    provider_label: str
    base_url: str
    model: str
    api_key_env: str
    role: str
    priority: int
    batch_size: int | None = None
    timeout_seconds: int | None = None


@dataclass(frozen=True, slots=True)
class ResolvedModelRouteStep:
    provider: str
    provider_label: str
    base_url: str
    model: str
    api_key: str
    api_key_env: str
    role: str
    priority: int
    batch_size: int | None = None
    timeout_seconds: int | None = None


MODEL_ROUTES: dict[str, tuple[ModelRouteStep, ...]] = {
    # All routes carry explicit per-step timeouts so a hung primary
    # never eats minutes of wall-time before fallback. 20s is generous
    # for DeepSeek's typical 2-5s response while still capping the
    # damage on a "responding but slow" day (we saw a single batch
    # take 6m10s on 2026-05-24 because no timeout was set).
    "dedupe_review": (
        ModelRouteStep("deepseek", "DeepSeek", DEEPSEEK_BASE_URL, DEEPSEEK_MODEL, "DEEPSEEK_API_KEY", "cheap_scoring", 1, timeout_seconds=20),
        ModelRouteStep("openai", "OpenAI", OPENAI_BASE_URL, OPENAI_SCORING_MODEL, "OPENAI_API_KEY", "scoring_fallback", 2, timeout_seconds=20),
        ModelRouteStep("groq", "Groq", GROQ_BASE_URL, GROQ_FALLBACK_MODEL, "GROQ_API_KEY", "resilient_fallback", 3, batch_size=6, timeout_seconds=20),
    ),
    "curator": (
        ModelRouteStep("deepseek", "DeepSeek", DEEPSEEK_BASE_URL, DEEPSEEK_MODEL, "DEEPSEEK_API_KEY", "cheap_scoring", 1, timeout_seconds=20),
        ModelRouteStep("openai", "OpenAI", OPENAI_BASE_URL, OPENAI_SCORING_MODEL, "OPENAI_API_KEY", "scoring_fallback", 2, timeout_seconds=30),
        ModelRouteStep("groq", "Groq", GROQ_BASE_URL, GROQ_FALLBACK_MODEL, "GROQ_API_KEY", "resilient_fallback", 3, batch_size=6, timeout_seconds=30),
    ),
    "rewrite": (
        # Visible Russian copy comes from OpenAI as the quality primary. We
        # make OpenAI itself robust first (small batches + generous timeout +
        # SDK backoff via max_retries in llm_rewrite), so it rarely misses.
        # DeepSeek sits at priority 2 as a LAST RESORT only: a slightly
        # weaker Russian sentence on a hard-news item (police appeal, cordon)
        # is better than the item vanishing. Fallback-written items keep
        # draft_line_provider="DeepSeek" so the degraded phrasing is auditable.
        ModelRouteStep("openai", "OpenAI", OPENAI_BASE_URL, OPENAI_REWRITE_MODEL, "OPENAI_API_KEY", "quality_rewrite_primary", 1, batch_size=6, timeout_seconds=60),
        ModelRouteStep("deepseek", "DeepSeek", DEEPSEEK_BASE_URL, DEEPSEEK_MODEL, "DEEPSEEK_API_KEY", "rewrite_last_resort", 2, batch_size=6, timeout_seconds=60),
    ),
    # English-first architecture: prepare compact English fact/reader cards
    # before any Russian copy is written. DeepSeek Pro is cheap enough for
    # broad source-language work and strong enough for extraction/synthesis;
    # OpenAI remains a fallback so a DeepSeek outage never blocks the run.
    "english_cards": (
        ModelRouteStep("deepseek", "DeepSeek", DEEPSEEK_BASE_URL, DEEPSEEK_MODEL, "DEEPSEEK_API_KEY", "english_fact_reader_fast", 1, batch_size=8, timeout_seconds=25),
        ModelRouteStep("openai", "OpenAI", OPENAI_BASE_URL, OPENAI_SCORING_MODEL, "OPENAI_API_KEY", "english_fact_reader_mini_fallback", 2, batch_size=6, timeout_seconds=30),
        ModelRouteStep("openai", "OpenAI", OPENAI_BASE_URL, OPENAI_REWRITE_MODEL, "OPENAI_API_KEY", "english_fact_reader_quality_fallback", 3, batch_size=6, timeout_seconds=60),
    ),
    # Translate only the already-formed English reader cards. This keeps the
    # expensive GPT call on the final short copy, not the raw evidence packet.
    # DeepSeek Pro is a fallback: weaker Russian is better than a vanished item,
    # and the line remains auditable via draft_line_provider/model.
    "final_translate": (
        ModelRouteStep("openai", "OpenAI", OPENAI_BASE_URL, OPENAI_SCORING_MODEL, "OPENAI_API_KEY", "final_ru_translation_mini_primary", 1, batch_size=8, timeout_seconds=30),
        ModelRouteStep("openai", "OpenAI", OPENAI_BASE_URL, OPENAI_REWRITE_MODEL, "OPENAI_API_KEY", "final_ru_translation_quality_fallback", 2, batch_size=6, timeout_seconds=60),
    ),
    # Transport: short structured translation → cheap mini is enough. Bigger
    # batches (short lines) + tight timeout; DeepSeek last-resort net.
    "transport_rewrite": (
        ModelRouteStep("openai", "OpenAI", OPENAI_BASE_URL, OPENAI_TRANSPORT_MODEL, "OPENAI_API_KEY", "transport_primary", 1, batch_size=6, timeout_seconds=30),
        ModelRouteStep("deepseek", "DeepSeek", DEEPSEEK_BASE_URL, DEEPSEEK_MODEL, "DEEPSEEK_API_KEY", "transport_last_resort", 2, batch_size=6, timeout_seconds=30),
    ),
    # Events carry structured datetime/venue fields. OpenAI stays primary;
    # DeepSeek is the same last-resort net so a non-deterministic culture
    # event (film, talk) never disappears just because OpenAI timed out.
    "events_rewrite": (
        ModelRouteStep("openai", "OpenAI", OPENAI_BASE_URL, OPENAI_REWRITE_MODEL, "OPENAI_API_KEY", "events_primary", 1, batch_size=5, timeout_seconds=60),
        ModelRouteStep("deepseek", "DeepSeek", DEEPSEEK_BASE_URL, DEEPSEEK_MODEL, "DEEPSEEK_API_KEY", "events_last_resort", 2, batch_size=5, timeout_seconds=60),
    ),
    "repair": (
        ModelRouteStep("openai", "OpenAI", OPENAI_BASE_URL, OPENAI_REWRITE_MODEL, "OPENAI_API_KEY", "quality_repair", 1, batch_size=5, timeout_seconds=60),
    ),
}


def is_deepseek_route(*, provider: str = "", model: str = "", base_url: str = "") -> bool:
    """Return True for DeepSeek-compatible chat calls.

    DeepSeek V4 has two production-critical quirks for this pipeline:
    thinking mode defaults on, and JSON mode should be requested explicitly
    for structured output. Keeping the detection here prevents each caller
    from re-implementing a slightly different policy.
    """
    provider_l = provider.lower()
    model_l = model.lower()
    base_l = base_url.lower().rstrip("/")
    return (
        provider_l == "deepseek"
        or model_l.startswith("deepseek")
        or base_l.startswith(DEEPSEEK_BASE_URL.rstrip("/").lower())
    )


def chat_completion_options_for_route(
    *,
    provider: str = "",
    model: str = "",
    base_url: str = "",
    json_mode: bool = True,
) -> dict[str, object]:
    """Provider-specific OpenAI-compatible request options.

    The project asks every LLM stage for machine-readable JSON. For DeepSeek
    we also disable thinking so a bad day fails quickly into the OpenAI mini
    reserve instead of spending minutes reasoning over a simple digest card.
    """
    if not is_deepseek_route(provider=provider, model=model, base_url=base_url):
        return {}
    options: dict[str, object] = {"extra_body": {"thinking": {"type": "disabled"}}}
    if json_mode:
        options["response_format"] = {"type": "json_object"}
    return options


def sdk_retries_for_route(*, provider: str = "", model: str = "", base_url: str = "") -> int:
    """SDK retries by provider.

    DeepSeek sits in front of mini/GPT fallback. Retrying the same slow
    DeepSeek request inside the SDK defeats that ladder, so DeepSeek gets no
    SDK retry; the pipeline retry only re-sends missing items once.
    """
    if is_deepseek_route(provider=provider, model=model, base_url=base_url):
        return 0
    return 1


def provider_label_for_model(model: str) -> str:
    if model.startswith("deepseek"):
        return "DeepSeek"
    if model.startswith("gpt-") or model.startswith("o1"):
        return "OpenAI"
    if model.startswith("llama") or "groq" in model.lower():
        return "Groq"
    return "unknown"


def resolve_model_route(
    route_name: str,
    *,
    provider_override: str = "",
    base_url_override: str = "",
    model_override: str = "",
) -> list[ResolvedModelRouteStep]:
    provider_override = provider_override.lower().strip()
    if provider_override == "none":
        return []
    if provider_override and base_url_override and model_override:
        return [
            ResolvedModelRouteStep(
                provider=provider_override,
                provider_label=provider_override.title(),
                base_url=base_url_override,
                model=model_override,
                api_key=os.environ.get("LLM_API_KEY", ""),
                api_key_env="LLM_API_KEY",
                role="manual_override",
                priority=1,
            )
        ]
    steps = MODEL_ROUTES.get(route_name, ())
    return [
        ResolvedModelRouteStep(
            provider=step.provider,
            provider_label=step.provider_label,
            base_url=step.base_url,
            model=step.model,
            api_key=os.environ.get(step.api_key_env, ""),
            api_key_env=step.api_key_env,
            role=step.role,
            priority=step.priority,
            batch_size=step.batch_size,
            timeout_seconds=step.timeout_seconds,
        )
        for step in steps
    ]


def route_snapshot() -> dict[str, list[dict[str, object]]]:
    return {
        name: [
            {key: value for key, value in asdict(step).items() if key != "api_key_env"}
            | {"api_key_env": step.api_key_env}
            for step in steps
        ]
        for name, steps in MODEL_ROUTES.items()
    }
