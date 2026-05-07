from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class DigestSection:
    title: str
    lines: list[str] = field(default_factory=list)


@dataclass(slots=True)
class DigestIssue:
    title: str
    subtitle: str
    sections: list[DigestSection] = field(default_factory=list)

    def render_text(self) -> str:
        rendered: list[str] = [self.title, self.subtitle, ""]

        for section in self.sections:
            rendered.append(section.title)
            for line in section.lines:
                rendered.append(f"- {line}")
            rendered.append("")

        return "\n".join(rendered).strip()

