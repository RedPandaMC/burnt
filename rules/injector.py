"""
Rules injection system for LLM sessions.
Provides mechanisms to bootstrap general rules into new LLM conversations.
"""

import yaml
from pathlib import Path
from typing import List, Dict, Optional
from dataclasses import dataclass


RULES_DIR = Path(__file__).parent
DEFAULT_RULES_FILE = RULES_DIR / "system_rules.yaml"


@dataclass
class Rule:
    id: str
    priority: int
    rule: str


class RulesInjector:
    def __init__(self, rules_file: Path = DEFAULT_RULES_FILE):
        self.rules_file = rules_file
        self._rules_cache: Optional[Dict] = None
    
    def _load_rules(self) -> Dict:
        if self._rules_cache is None:
            with open(self.rules_file, 'r') as f:
                self._rules_cache = yaml.safe_load(f)
        return self._rules_cache or {}
    
    def get_all_rules_text(self, include_categories: List[str] = None) -> str:
        if include_categories is None:
            include_categories = ['core_rules', 'collaboration_rules', 'output_format_rules']
        
        rules_data = self._load_rules()
        
        output = ["=== GENERAL RULES FOR LLM SESSION ===\n"]
        
        for category in include_categories:
            if category in rules_data.get('llm_general_rules', {}):
                output.append(f"\n## {category.replace('_', ' ').title()}\n")
                for rule in rules_data['llm_general_rules'][category]:
                    output.append(f"- {rule['rule'].strip()}\n")
        
        output.append("\n=== END RULES ===")
        return "".join(output)
    
    def get_system_prompt(self) -> str:
        """Generate a system prompt with all rules injected."""
        rules_text = self.get_all_rules_text()
        return f"""You are an AI assistant following these operational rules:

{rules_text}

Remember: Always adhere to these rules throughout this conversation. When uncertain, prioritize safety, transparency, and collaboration."""
    
    def get_rules_as_xml(self) -> str:
        """Get rules in XML format for systems that use XML prompts."""
        rules_data = self._load_rules()
        
        xml_lines = ['<rules>', '  <version>1.0</version>']
        
        for category in ['core_rules', 'collaboration_rules', 'output_format_rules']:
            if category in rules_data.get('llm_general_rules', {}):
                xml_lines.append(f'  <category name="{category}">')
                for rule in rules_data['llm_general_rules'][category]:
                    xml_lines.append(f'    <rule id="{rule["id"]}" priority="{rule["priority"]}">')
                    xml_lines.append(f'      <content><![CDATA[{rule["rule"].strip()}]]></content>')
                    xml_lines.append('    </rule>')
                xml_lines.append('  </category>')
        
        xml_lines.append('</rules>')
        return '\n'.join(xml_lines)
    
    def get_rule_by_id(self, rule_id: str) -> Optional[Rule]:
        """Get a specific rule by its ID."""
        rules_data = self._load_rules()
        
        for category in ['core_rules', 'collaboration_rules', 'output_format_rules']:
            if category in rules_data.get('llm_general_rules', {}):
                for rule in rules_data['llm_general_rules'][category]:
                    if rule['id'] == rule_id:
                        return Rule(
                            id=rule['id'],
                            priority=rule['priority'],
                            rule=rule['rule']
                        )
        return None


_default_injector: Optional[RulesInjector] = None


def get_default_injector() -> RulesInjector:
    global _default_injector
    if _default_injector is None:
        _default_injector = RulesInjector()
    return _default_injector


def inject_rules_into_session(session_config: Dict) -> Dict:
    """
    Inject rules into a session configuration.
    
    Args:
        session_config: Dict with 'system_prompt' key or similar
        
    Returns:
        Updated session config with rules injected
    """
    injector = get_default_injector()
    rules_prompt = injector.get_system_prompt()
    
    if 'system_prompt' in session_config:
        session_config['system_prompt'] = f"{rules_prompt}\n\n---\n\n{session_config['system_prompt']}"
    else:
        session_config['system_prompt'] = rules_prompt
    
    return session_config


def get_bootstrap_prompt() -> str:
    """Get the bootstrap prompt for new LLM agents."""
    return get_default_injector().get_system_prompt()


if __name__ == "__main__":
    injector = RulesInjector()
    print(injector.get_system_prompt())