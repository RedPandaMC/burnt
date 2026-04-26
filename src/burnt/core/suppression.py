"""Comment-based suppression: parse and apply # burnt: ignore directives."""

from __future__ import annotations

import re

from .rule_filter import RuleIndex

# # burnt: ignore  OR  # burnt: ignore[BP008, performance]
_LINE_RE = re.compile(r"#\s*burnt:\s*ignore(?:\[([^\]]*)\])?", re.IGNORECASE)
# # burnt: ignore-file  OR  # burnt: ignore-file[pyspark]
_FILE_RE = re.compile(r"#\s*burnt:\s*ignore-file(?:\[([^\]]*)\])?", re.IGNORECASE)


def parse_suppressions(
    source: str,
    index: RuleIndex,
) -> tuple[frozenset[str], dict[int, frozenset[str]], frozenset[int]]:
    """Parse suppression comments from source.

    Returns:
        (file_suppressed, line_suppressed, standalone_lines) where:
        - file_suppressed: rule codes suppressed for the whole file
        - line_suppressed: 1-based line number → suppressed rule codes
        - standalone_lines: line numbers where the suppress comment is the only
          content (so it propagates to the next line)
    """
    lines = source.splitlines()
    file_suppressed: set[str] = set()
    line_suppressed: dict[int, frozenset[str]] = {}
    standalone_lines: set[int] = set()

    for i, line in enumerate(lines, 1):
        fm = _FILE_RE.search(line)
        if fm:
            raw = fm.group(1) or "ALL"
            for p in (p.strip() for p in raw.split(",")):
                if p:
                    file_suppressed |= index.resolve_pattern(p)

        lm = _LINE_RE.search(line)
        if lm:
            raw = lm.group(1) or "ALL"
            suppressed: set[str] = set()
            for p in (p.strip() for p in raw.split(",")):
                if p:
                    suppressed |= index.resolve_pattern(p)
            line_suppressed[i] = frozenset(suppressed)
            # standalone: line has no code before the comment
            if line.strip().startswith("#"):
                standalone_lines.add(i)

    return frozenset(file_suppressed), line_suppressed, frozenset(standalone_lines)


def apply_suppressions(
    findings: list,
    file_suppressed: frozenset[str],
    line_suppressed: dict[int, frozenset[str]],
    standalone_lines: frozenset[int] = frozenset(),
) -> list:
    """Filter out findings that are suppressed by comments."""
    result = []
    for f in findings:
        if f.name in file_suppressed:
            continue
        ln = f.line_number
        if ln is not None:
            if f.name in line_suppressed.get(ln, frozenset()):
                continue
            # standalone comment on the previous line suppresses this line
            prev = ln - 1
            if prev in standalone_lines and f.name in line_suppressed.get(prev, frozenset()):
                continue
        result.append(f)
    return result
