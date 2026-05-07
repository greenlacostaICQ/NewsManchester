from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True, slots=True)
class AgentSpec:
    stage: str
    agent: str
    model: str
    reasoning_effort: str
    purpose: str
    hard_limits: list[str]


AGENT_SPECS: list[AgentSpec] = [
    # Descriptive only: Codex reads this as external model policy, not
    # runtime API calls. Python stages still do the mechanical work.
    # Mini handles cheap structured review; full model writes Russian text.
    AgentSpec(
        stage="collect",
        agent="collector",
        model="GPT-5.4-Mini",
        reasoning_effort="low",
        purpose="After collect-digest, review candidates.json for obvious non-GM/index/category noise.",
        hard_limits=[
            "Use collector_report.json only for source-health context.",
            "Edit only clear wrong includes.",
            "If uncertain, leave Python output as-is.",
        ],
    ),
    AgentSpec(
        stage="dedupe",
        agent="dedupe-classifier",
        model="GPT-5.4-Mini",
        reasoning_effort="medium",
        purpose="After dedupe-digest, review ambiguous edge cases in dedupe_memory.json.",
        hard_limits=[
            "Do not rewrite normal deterministic decisions.",
            "Only correct obvious carry-over/new-phase mistakes.",
            "If uncertain, leave Python output as-is.",
        ],
    ),
    AgentSpec(
        stage="validate",
        agent="candidate-validator",
        model="GPT-5.4-Mini",
        reasoning_effort="medium",
        purpose="After validate-candidates, lightly review candidates.json for weak include=true items.",
        hard_limits=[
            "No prose rewriting.",
            "Only reject obvious homepage/search/category/non-GM items.",
            "Do not mass-edit candidates.json.",
        ],
    ),
    AgentSpec(
        stage="write",
        agent="writer",
        model="GPT-5.4",
        reasoning_effort="medium",
        purpose="Write normal Russian draft_line values for every include=true validated candidate that can appear in the digest.",
        hard_limits=[
            "Cannot invent facts, links or timings.",
            "Cannot use candidates without source_url/source_label.",
            "Do not write source anchor HTML; write-digest attaches links.",
            "Do not leave transport/culture/football/ticket items to generic fallback prose.",
        ],
    ),
    AgentSpec(
        stage="edit",
        agent="editor-balancer",
        model="GPT-5.4",
        reasoning_effort="medium",
        purpose="Repair technical draft issues and reduce soft-layer skew.",
        hard_limits=[
            "Cannot add new facts absent from candidates.",
            "Must remove duplicate meaning collisions.",
            "Must fail if city/public-affairs layer is materially weak.",
        ],
    ),
    AgentSpec(
        stage="release",
        agent="red-team-gate",
        model="GPT-5.4",
        reasoning_effort="high",
        purpose="Final PASS/FAIL decision.",
        hard_limits=[
            "Fail closed.",
            "No almost-pass state.",
            "Must block release if the draft goes out without real city news.",
        ],
    ),
]


def pipeline_config_payload() -> dict[str, object]:
    return {"agents": [asdict(spec) for spec in AGENT_SPECS]}
