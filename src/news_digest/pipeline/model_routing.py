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
# Prose rewrite defaults to mini. gpt-4o is no longer a broad production
# fallback: one slow morning on 2026-06-17 showed that automatic escalation of
# wide batches can consume the whole send window. The rewrite code may call
# gpt-4o surgically for the single lead item only; all normal board/translation
# work must fit mini or degrade optional items into backup.
OPENAI_REWRITE_MODEL = "gpt-4o"
OPENAI_SCORING_MODEL = "gpt-4o-mini"
# Kept for explicit manual overrides; the default morning transport route is
# mini-only and relies on deterministic transport_fill before rewrite.
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
        ModelRouteStep("openai", "OpenAI", OPENAI_BASE_URL, OPENAI_SCORING_MODEL, "OPENAI_API_KEY", "curator_mini_primary", 1, timeout_seconds=30),
    ),
    "rewrite": (
        ModelRouteStep("openai", "OpenAI", OPENAI_BASE_URL, OPENAI_SCORING_MODEL, "OPENAI_API_KEY", "mini_rewrite_primary", 1, batch_size=6, timeout_seconds=45),
    ),
    # English-first architecture: judge source-language candidates and prepare
    # compact English fact/reader cards before any Russian copy is written.
    # OpenAI mini is the primary board judge; gpt-4o is a surgical single-lead
    # fallback only, never a broad extraction fallback.
    "english_cards": (
        ModelRouteStep("openai", "OpenAI", OPENAI_BASE_URL, OPENAI_SCORING_MODEL, "OPENAI_API_KEY", "board_judge_mini_primary", 1, batch_size=8, timeout_seconds=30),
        ModelRouteStep("openai", "OpenAI", OPENAI_BASE_URL, OPENAI_REWRITE_MODEL, "OPENAI_API_KEY", "lead_only_board_fallback", 2, batch_size=1, timeout_seconds=45),
    ),
    # Translate only the already-formed English reader cards. This keeps the
    # expensive GPT call on the final short copy, not the raw evidence packet.
    # gpt-4o is a surgical single-lead fallback only.
    "final_translate": (
        ModelRouteStep("openai", "OpenAI", OPENAI_BASE_URL, OPENAI_SCORING_MODEL, "OPENAI_API_KEY", "final_ru_translation_mini_primary", 1, batch_size=8, timeout_seconds=30),
        ModelRouteStep("openai", "OpenAI", OPENAI_BASE_URL, OPENAI_REWRITE_MODEL, "OPENAI_API_KEY", "lead_only_translation_fallback", 2, batch_size=1, timeout_seconds=45),
    ),
    # Transport: short structured translation → cheap mini is enough. Most
    # transport lines should already be handled by transport_fill.
    "transport_rewrite": (
        ModelRouteStep("openai", "OpenAI", OPENAI_BASE_URL, OPENAI_SCORING_MODEL, "OPENAI_API_KEY", "transport_mini_primary", 1, batch_size=6, timeout_seconds=30),
    ),
    # Events carry structured datetime/venue fields; mini gets the compact
    # selected board only.
    "events_rewrite": (
        ModelRouteStep("openai", "OpenAI", OPENAI_BASE_URL, OPENAI_SCORING_MODEL, "OPENAI_API_KEY", "events_mini_primary", 1, batch_size=5, timeout_seconds=45),
    ),
    "repair": (
        ModelRouteStep("openai", "OpenAI", OPENAI_BASE_URL, OPENAI_SCORING_MODEL, "OPENAI_API_KEY", "hard_defect_repair_mini", 1, batch_size=5, timeout_seconds=30),
        ModelRouteStep("openai", "OpenAI", OPENAI_BASE_URL, OPENAI_REWRITE_MODEL, "OPENAI_API_KEY", "lead_only_repair_fallback", 2, batch_size=1, timeout_seconds=45),
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
