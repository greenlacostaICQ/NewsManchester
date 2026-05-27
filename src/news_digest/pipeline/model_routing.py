"""Central model routing policy for LLM-backed pipeline decisions.

The policy keeps cheap classification/reject tasks separate from higher
stakes rewrite tasks. Environment overrides still work for local debugging,
but the default route is explicit and reportable.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import os

DEEPSEEK_BASE_URL = "https://api.deepseek.com/v1"
DEEPSEEK_MODEL = "deepseek-chat"
OPENAI_BASE_URL = "https://api.openai.com/v1"
OPENAI_REWRITE_MODEL = "gpt-4o-mini"
OPENAI_SCORING_MODEL = "gpt-4o-mini"
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
        ModelRouteStep("openai", "OpenAI", OPENAI_BASE_URL, OPENAI_REWRITE_MODEL, "OPENAI_API_KEY", "quality_rewrite_primary", 1, batch_size=12, timeout_seconds=20),
        ModelRouteStep("deepseek", "DeepSeek", DEEPSEEK_BASE_URL, DEEPSEEK_MODEL, "DEEPSEEK_API_KEY", "fast_rewrite_fallback", 2, batch_size=12, timeout_seconds=20),
        # Groq batch_size dropped 3 -> 2 to stay under the 12k TPM
        # limit; a 3-batch rewrite payload was hitting 12048 on 2026-05-24.
        ModelRouteStep("groq", "Groq", GROQ_BASE_URL, GROQ_FALLBACK_MODEL, "GROQ_API_KEY", "resilient_fallback", 3, batch_size=2, timeout_seconds=30),
    ),
    # Events have structured datetime/venue fields where DeepSeek
    # historically degrades into the fallback chain; putting OpenAI
    # first short-circuits the 90s primary timeout we used to eat.
    # Explicit per-step timeouts cap wall-time per item even on bad days.
    "events_rewrite": (
        ModelRouteStep("openai", "OpenAI", OPENAI_BASE_URL, OPENAI_REWRITE_MODEL, "OPENAI_API_KEY", "events_primary", 1, batch_size=8, timeout_seconds=20),
        ModelRouteStep("groq", "Groq", GROQ_BASE_URL, GROQ_FALLBACK_MODEL, "GROQ_API_KEY", "events_resilient_fallback", 2, batch_size=3, timeout_seconds=20),
        ModelRouteStep("deepseek", "DeepSeek", DEEPSEEK_BASE_URL, DEEPSEEK_MODEL, "DEEPSEEK_API_KEY", "events_last_resort", 3, batch_size=8, timeout_seconds=30),
    ),
    "repair": (
        ModelRouteStep("openai", "OpenAI", OPENAI_BASE_URL, OPENAI_REWRITE_MODEL, "OPENAI_API_KEY", "quality_repair", 1, batch_size=7, timeout_seconds=30),
        ModelRouteStep("deepseek", "DeepSeek", DEEPSEEK_BASE_URL, DEEPSEEK_MODEL, "DEEPSEEK_API_KEY", "repair_fallback", 2, batch_size=7, timeout_seconds=20),
        ModelRouteStep("groq", "Groq", GROQ_BASE_URL, GROQ_FALLBACK_MODEL, "GROQ_API_KEY", "resilient_fallback", 3, batch_size=2, timeout_seconds=30),
    ),
}


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
