"""
Rule Engine Framework for I-9 Processing

This module provides a comprehensive rule engine for processing I-9 forms
with complex business logic and validation scenarios.
"""

from .rule_engine import RuleEngine, Rule, RuleResult
from .i9_rules import I9BusinessRules
from .scenario_processor import ScenarioProcessor

__all__ = [
    'RuleEngine',
    'Rule', 
    'RuleResult',
    'I9BusinessRules',
    'ScenarioProcessor'
]
