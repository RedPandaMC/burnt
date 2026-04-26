"""Ruff-like rule selection: resolve patterns (exact ID, prefix, tag, ALL) to rule codes."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RuleIndex:
    """Immutable index built once from list_rules()."""

    by_code: dict[str, object]
    by_tag: dict[str, frozenset[str]]
    all_codes: frozenset[str]

    @classmethod
    def build(cls) -> "RuleIndex":
        from burnt._engine import list_rules

        rules = list_rules()
        by_code: dict[str, object] = {r.code: r for r in rules}
        by_tag: dict[str, set[str]] = {}
        for r in rules:
            for tag in r.tags:
                by_tag.setdefault(tag, set()).add(r.code)
        return cls(
            by_code=by_code,
            by_tag={k: frozenset(v) for k, v in by_tag.items()},
            all_codes=frozenset(by_code),
        )

    def resolve_pattern(self, pattern: str) -> frozenset[str]:
        """Expand one pattern to a set of matching rule codes.

        Supports: "ALL", exact code ("BP008"), tag ("performance"), prefix ("BP").
        """
        if pattern == "ALL":
            return self.all_codes
        if pattern in self.by_code:
            return frozenset({pattern})
        if pattern in self.by_tag:
            return self.by_tag[pattern]
        # prefix match
        return frozenset(c for c in self.all_codes if c.startswith(pattern))

    def resolve_active(
        self,
        select: list[str],
        extend_select: list[str],
        ignore: list[str],
        extend_ignore: list[str],
    ) -> frozenset[str]:
        """Resolve the final active rule set from select/ignore patterns."""
        base: set[str] = set()
        for p in select:
            base |= self.resolve_pattern(p)
        for p in extend_select:
            base |= self.resolve_pattern(p)
        for p in ignore + extend_ignore:
            base -= self.resolve_pattern(p)
        return frozenset(base)
