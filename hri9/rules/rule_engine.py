#!/usr/bin/env python3
"""
Core Rule Engine Framework

This module provides the foundational rule engine infrastructure for
executing complex business rules and validation logic.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Callable, Union
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class RuleStatus(Enum):
    """Status of rule execution"""
    PASSED = "PASSED"
    FAILED = "FAILED"
    WARNING = "WARNING"
    SKIPPED = "SKIPPED"
    ERROR = "ERROR"


class RuleSeverity(Enum):
    """Severity level of rule violations"""
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    INFO = "INFO"


@dataclass
class RuleResult:
    """Result of a rule execution"""
    rule_id: str
    rule_name: str
    status: RuleStatus
    severity: RuleSeverity
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    recommendations: List[str] = field(default_factory=list)
    execution_time_ms: float = 0.0
    
    @property
    def is_success(self) -> bool:
        """Check if rule passed successfully"""
        return self.status == RuleStatus.PASSED
    
    @property
    def is_failure(self) -> bool:
        """Check if rule failed"""
        return self.status == RuleStatus.FAILED
    
    @property
    def is_critical(self) -> bool:
        """Check if rule failure is critical"""
        return self.severity == RuleSeverity.CRITICAL


@dataclass
class RuleContext:
    """Context passed to rules during execution"""
    document_data: Dict[str, Any]
    processing_metadata: Dict[str, Any] = field(default_factory=dict)
    user_config: Dict[str, Any] = field(default_factory=dict)
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get value from document data"""
        return self.document_data.get(key, default)
    
    def set_metadata(self, key: str, value: Any) -> None:
        """Set processing metadata"""
        self.processing_metadata[key] = value
    
    def get_metadata(self, key: str, default: Any = None) -> Any:
        """Get processing metadata"""
        return self.processing_metadata.get(key, default)


class Rule(ABC):
    """Abstract base class for all rules"""
    
    def __init__(self, rule_id: str, name: str, severity: RuleSeverity = RuleSeverity.MEDIUM,
                 enabled: bool = True, dependencies: List[str] = None):
        self.rule_id = rule_id
        self.name = name
        self.severity = severity
        self.enabled = enabled
        self.dependencies = dependencies or []
    
    @abstractmethod
    def execute(self, context: RuleContext) -> RuleResult:
        """Execute the rule logic"""
        pass
    
    def should_execute(self, context: RuleContext) -> bool:
        """Determine if rule should be executed based on context"""
        return self.enabled
    
    def create_result(self, status: RuleStatus, message: str, 
                     details: Dict[str, Any] = None, 
                     recommendations: List[str] = None) -> RuleResult:
        """Helper to create rule result"""
        return RuleResult(
            rule_id=self.rule_id,
            rule_name=self.name,
            status=status,
            severity=self.severity,
            message=message,
            details=details or {},
            recommendations=recommendations or []
        )


class ConditionalRule(Rule):
    """Rule that executes based on a condition"""
    
    def __init__(self, rule_id: str, name: str, condition: Callable[[RuleContext], bool],
                 rule_func: Callable[[RuleContext], RuleResult], **kwargs):
        super().__init__(rule_id, name, **kwargs)
        self.condition = condition
        self.rule_func = rule_func
    
    def should_execute(self, context: RuleContext) -> bool:
        """Check both enabled status and condition"""
        return super().should_execute(context) and self.condition(context)
    
    def execute(self, context: RuleContext) -> RuleResult:
        """Execute the conditional rule"""
        if not self.should_execute(context):
            return self.create_result(
                RuleStatus.SKIPPED,
                f"Rule {self.name} skipped - condition not met"
            )
        
        return self.rule_func(context)


@dataclass
class RuleExecutionSummary:
    """Summary of rule execution results"""
    total_rules: int
    passed: int
    failed: int
    warnings: int
    skipped: int
    errors: int
    critical_failures: int
    execution_time_ms: float
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate percentage"""
        if self.total_rules == 0:
            return 0.0
        return (self.passed / self.total_rules) * 100
    
    @property
    def has_critical_failures(self) -> bool:
        """Check if there are any critical failures"""
        return self.critical_failures > 0


class RuleEngine:
    """Main rule engine for executing business rules"""
    
    def __init__(self, config: Dict[str, Any] = None):
        self.rules: Dict[str, Rule] = {}
        self.rule_groups: Dict[str, List[str]] = {}
        self.config = config or {}
        self.logger = logging.getLogger(f"{__name__}.RuleEngine")
    
    def register_rule(self, rule: Rule, group: str = "default") -> None:
        """Register a rule with the engine"""
        self.rules[rule.rule_id] = rule
        
        if group not in self.rule_groups:
            self.rule_groups[group] = []
        self.rule_groups[group].append(rule.rule_id)
        
        self.logger.debug(f"Registered rule {rule.rule_id} in group {group}")
    
    def register_rules(self, rules: List[Rule], group: str = "default") -> None:
        """Register multiple rules"""
        for rule in rules:
            self.register_rule(rule, group)
    
    def execute_rule(self, rule_id: str, context: RuleContext) -> RuleResult:
        """Execute a single rule"""
        import time
        
        if rule_id not in self.rules:
            return RuleResult(
                rule_id=rule_id,
                rule_name="Unknown",
                status=RuleStatus.ERROR,
                severity=RuleSeverity.HIGH,
                message=f"Rule {rule_id} not found"
            )
        
        rule = self.rules[rule_id]
        
        if not rule.should_execute(context):
            return RuleResult(
                rule_id=rule_id,
                rule_name=rule.name,
                status=RuleStatus.SKIPPED,
                severity=rule.severity,
                message=f"Rule {rule.name} was skipped"
            )
        
        start_time = time.time()
        
        try:
            result = rule.execute(context)
            result.execution_time_ms = (time.time() - start_time) * 1000
            
            self.logger.debug(f"Rule {rule_id} executed: {result.status.value}")
            return result
            
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            self.logger.error(f"Error executing rule {rule_id}: {e}")
            
            return RuleResult(
                rule_id=rule_id,
                rule_name=rule.name,
                status=RuleStatus.ERROR,
                severity=RuleSeverity.HIGH,
                message=f"Rule execution failed: {str(e)}",
                execution_time_ms=execution_time
            )
    
    def execute_group(self, group: str, context: RuleContext, 
                     stop_on_critical: bool = False) -> List[RuleResult]:
        """Execute all rules in a group"""
        if group not in self.rule_groups:
            self.logger.warning(f"Rule group {group} not found")
            return []
        
        results = []
        rule_ids = self.rule_groups[group]
        
        # Sort rules by dependencies (simple topological sort)
        sorted_rule_ids = self._sort_rules_by_dependencies(rule_ids)
        
        for rule_id in sorted_rule_ids:
            result = self.execute_rule(rule_id, context)
            results.append(result)
            
            # Stop execution on critical failure if requested
            if stop_on_critical and result.is_critical and result.is_failure:
                self.logger.warning(f"Stopping execution due to critical failure in rule {rule_id}")
                break
        
        return results
    
    def execute_all(self, context: RuleContext, 
                   stop_on_critical: bool = False) -> Dict[str, List[RuleResult]]:
        """Execute all registered rules grouped by their groups"""
        all_results = {}
        
        for group in self.rule_groups:
            all_results[group] = self.execute_group(group, context, stop_on_critical)
            
            # Check for critical failures across groups
            if stop_on_critical:
                critical_failures = [r for r in all_results[group] 
                                   if r.is_critical and r.is_failure]
                if critical_failures:
                    self.logger.warning(f"Stopping all execution due to critical failures in group {group}")
                    break
        
        return all_results
    
    def get_execution_summary(self, results: Union[List[RuleResult], Dict[str, List[RuleResult]]]) -> RuleExecutionSummary:
        """Generate execution summary from results"""
        if isinstance(results, dict):
            # Flatten results from all groups
            all_results = []
            for group_results in results.values():
                all_results.extend(group_results)
            results = all_results
        
        total_rules = len(results)
        passed = sum(1 for r in results if r.status == RuleStatus.PASSED)
        failed = sum(1 for r in results if r.status == RuleStatus.FAILED)
        warnings = sum(1 for r in results if r.status == RuleStatus.WARNING)
        skipped = sum(1 for r in results if r.status == RuleStatus.SKIPPED)
        errors = sum(1 for r in results if r.status == RuleStatus.ERROR)
        critical_failures = sum(1 for r in results if r.is_critical and r.is_failure)
        total_time = sum(r.execution_time_ms for r in results)
        
        return RuleExecutionSummary(
            total_rules=total_rules,
            passed=passed,
            failed=failed,
            warnings=warnings,
            skipped=skipped,
            errors=errors,
            critical_failures=critical_failures,
            execution_time_ms=total_time
        )
    
    def _sort_rules_by_dependencies(self, rule_ids: List[str]) -> List[str]:
        """Simple topological sort for rule dependencies"""
        # For now, return as-is. Can be enhanced for complex dependency graphs
        return rule_ids
    
    def get_rule_info(self, rule_id: str) -> Dict[str, Any]:
        """Get information about a specific rule"""
        if rule_id not in self.rules:
            return {}
        
        rule = self.rules[rule_id]
        return {
            'rule_id': rule.rule_id,
            'name': rule.name,
            'severity': rule.severity.value,
            'enabled': rule.enabled,
            'dependencies': rule.dependencies
        }
    
    def list_rules(self, group: str = None) -> List[Dict[str, Any]]:
        """List all rules or rules in a specific group"""
        if group:
            if group not in self.rule_groups:
                return []
            rule_ids = self.rule_groups[group]
        else:
            rule_ids = list(self.rules.keys())
        
        return [self.get_rule_info(rule_id) for rule_id in rule_ids]
