"""
Rules injection system for LLM sessions.
"""

from .injector import (
    RulesInjector,
    get_default_injector,
    inject_rules_into_session,
    get_bootstrap_prompt,
)

__all__ = [
    "RulesInjector",
    "get_default_injector",
    "inject_rules_into_session",
    "get_bootstrap_prompt",
]