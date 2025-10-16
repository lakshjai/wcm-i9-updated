"""
Business Rules Reporter

This module handles the generation and export of business rules processing reports.
"""

import json
import os
from datetime import datetime
from typing import Dict, List, Any, Optional
from pathlib import Path

from hri9.core.models import ProcessingResult, ScenarioResult
from hri9.config.settings import BUSINESS_RULES_OUTPUT_DIR


class BusinessRulesReporter:
    """Generate comprehensive business rules reports"""
    
    @staticmethod
    def generate_business_rules_report(processing_result: ProcessingResult, 
                                     employee_id: str, 
                                     pdf_filename: str) -> Dict[str, Any]:
        """
        Generate a comprehensive business rules report for a single document.
        
        Args:
            processing_result: ProcessingResult with scenario and validation data
            employee_id: Employee identifier
            pdf_filename: Name of the processed PDF file
            
        Returns:
            Dictionary containing the complete business rules report
        """
        
        report = {
            "report_metadata": {
                "employee_id": employee_id,
                "pdf_filename": pdf_filename,
                "report_timestamp": datetime.now().isoformat(),
                "report_version": "1.0",
                "processing_status": processing_result.status.value
            },
            "processing_summary": {
                "total_scenarios_processed": len(processing_result.scenario_results),
                "total_validations": processing_result.total_validations,
                "passed_validations": processing_result.passed_validations,
                "failed_validations": processing_result.failed_validations,
                "critical_issues": processing_result.critical_issues,
                "validation_success_rate": processing_result.validation_success_rate,
                "overall_status": processing_result.status.value
            },
            "scenario_results": [],
            "validation_summary": {
                "field_validations": [],
                "cross_field_validations": [],
                "document_validations": [],
                "compliance_validations": []
            },
            "form_analysis": {
                "primary_form_detected": processing_result.primary_i9_data is not None,
                "form_type_selected": processing_result.form_type_selected,
                "selection_reason": processing_result.selection_reason,
                "total_forms_detected": processing_result.total_forms_detected
            },
            "recommendations": [],
            "next_steps": []
        }
        
        # Process scenario results
        for scenario in processing_result.scenario_results:
            scenario_data = {
                "scenario_name": scenario.scenario_name,
                "scenario_id": getattr(scenario, 'scenario_id', 'unknown'),
                "status": scenario.status.value,
                "has_critical_issues": scenario.has_critical_issues,
                "validation_results": [],
                "notes": scenario.notes,
                "processing_time": getattr(scenario, 'processing_time', 0.0)
            }
            
            # Add validation results for this scenario
            for validation in scenario.validation_results:
                validation_data = {
                    "rule_name": validation.validation_type,
                    "is_valid": validation.is_valid,
                    "severity": validation.severity,
                    "message": validation.message,
                    "details": validation.details
                }
                scenario_data["validation_results"].append(validation_data)
                
                # Categorize validations
                if "field" in validation.validation_type.lower():
                    report["validation_summary"]["field_validations"].append(validation_data)
                elif "document" in validation.validation_type.lower():
                    report["validation_summary"]["document_validations"].append(validation_data)
                elif "compliance" in validation.validation_type.lower():
                    report["validation_summary"]["compliance_validations"].append(validation_data)
                else:
                    report["validation_summary"]["cross_field_validations"].append(validation_data)
            
            report["scenario_results"].append(scenario_data)
        
        # Generate recommendations based on results
        report["recommendations"] = BusinessRulesReporter._generate_recommendations(processing_result)
        report["next_steps"] = BusinessRulesReporter._generate_next_steps(processing_result)
        
        return report
    
    @staticmethod
    def _generate_recommendations(processing_result: ProcessingResult) -> List[str]:
        """Generate actionable recommendations based on processing results"""
        recommendations = []
        
        if processing_result.critical_issues > 0:
            recommendations.append("CRITICAL: Address critical validation failures before proceeding")
        
        if processing_result.failed_validations > processing_result.passed_validations:
            recommendations.append("Review and correct failed validations to improve compliance")
        
        if processing_result.validation_success_rate < 50.0:
            recommendations.append("Low validation success rate - manual review recommended")
        
        if not processing_result.primary_i9_data:
            recommendations.append("No I-9 form detected - verify document contains valid I-9 forms")
        
        if len(processing_result.scenario_results) == 0:
            recommendations.append("No applicable scenarios found - review document structure")
        
        if not recommendations:
            recommendations.append("Processing completed successfully - no immediate action required")
        
        return recommendations
    
    @staticmethod
    def _generate_next_steps(processing_result: ProcessingResult) -> List[str]:
        """Generate next steps based on processing results"""
        next_steps = []
        
        if processing_result.status.value == "ERROR":
            next_steps.extend([
                "1. Review error details and processing logs",
                "2. Verify document quality and format",
                "3. Consider manual processing if automated processing fails"
            ])
        elif processing_result.status.value == "PARTIAL_SUCCESS":
            next_steps.extend([
                "1. Review validation failures and warnings",
                "2. Correct identified issues if possible",
                "3. Proceed with caution for critical business processes"
            ])
        elif processing_result.status.value == "COMPLETE_SUCCESS":
            next_steps.extend([
                "1. Document is ready for business use",
                "2. Archive processed results",
                "3. Update employee records as needed"
            ])
        else:
            next_steps.append("1. Review processing status and take appropriate action")
        
        return next_steps
    
    @staticmethod
    def save_business_rules_report(report: Dict[str, Any], 
                                 employee_id: str, 
                                 output_format: str = "json") -> Optional[str]:
        """
        Save business rules report to file.
        
        Args:
            report: Business rules report dictionary
            employee_id: Employee identifier for filename
            output_format: Output format ('json' or 'txt')
            
        Returns:
            Path to saved file, or None if save failed
        """
        
        try:
            # Ensure output directory exists
            os.makedirs(BUSINESS_RULES_OUTPUT_DIR, exist_ok=True)
            
            # Generate filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            if output_format.lower() == "json":
                filename = f"business_rules_{employee_id}_{timestamp}.json"
                filepath = os.path.join(BUSINESS_RULES_OUTPUT_DIR, filename)
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(report, f, indent=2, ensure_ascii=False)
                    
            elif output_format.lower() == "txt":
                filename = f"business_rules_{employee_id}_{timestamp}.txt"
                filepath = os.path.join(BUSINESS_RULES_OUTPUT_DIR, filename)
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    BusinessRulesReporter._write_text_report(f, report)
            else:
                raise ValueError(f"Unsupported output format: {output_format}")
            
            return filepath
            
        except Exception as e:
            print(f"Error saving business rules report: {e}")
            return None
    
    @staticmethod
    def _write_text_report(file_handle, report: Dict[str, Any]):
        """Write a human-readable text report"""
        
        # Header
        file_handle.write("=" * 80 + "\n")
        file_handle.write("BUSINESS RULES PROCESSING REPORT\n")
        file_handle.write("=" * 80 + "\n\n")
        
        # Metadata
        metadata = report["report_metadata"]
        file_handle.write(f"Employee ID: {metadata['employee_id']}\n")
        file_handle.write(f"PDF File: {metadata['pdf_filename']}\n")
        file_handle.write(f"Report Generated: {metadata['report_timestamp']}\n")
        file_handle.write(f"Processing Status: {metadata['processing_status']}\n\n")
        
        # Processing Summary
        summary = report["processing_summary"]
        file_handle.write("PROCESSING SUMMARY\n")
        file_handle.write("-" * 40 + "\n")
        file_handle.write(f"Scenarios Processed: {summary['total_scenarios_processed']}\n")
        file_handle.write(f"Total Validations: {summary['total_validations']}\n")
        file_handle.write(f"Passed Validations: {summary['passed_validations']}\n")
        file_handle.write(f"Failed Validations: {summary['failed_validations']}\n")
        file_handle.write(f"Critical Issues: {summary['critical_issues']}\n")
        file_handle.write(f"Success Rate: {summary['validation_success_rate']:.1f}%\n\n")
        
        # Scenario Results
        file_handle.write("SCENARIO RESULTS\n")
        file_handle.write("-" * 40 + "\n")
        for scenario in report["scenario_results"]:
            file_handle.write(f"Scenario: {scenario['scenario_name']}\n")
            file_handle.write(f"  Status: {scenario['status']}\n")
            file_handle.write(f"  Critical Issues: {scenario['has_critical_issues']}\n")
            file_handle.write(f"  Validations: {len(scenario['validation_results'])}\n")
            if scenario['notes']:
                file_handle.write(f"  Notes: {scenario['notes']}\n")
            file_handle.write("\n")
        
        # Recommendations
        file_handle.write("RECOMMENDATIONS\n")
        file_handle.write("-" * 40 + "\n")
        for i, rec in enumerate(report["recommendations"], 1):
            file_handle.write(f"{i}. {rec}\n")
        file_handle.write("\n")
        
        # Next Steps
        file_handle.write("NEXT STEPS\n")
        file_handle.write("-" * 40 + "\n")
        for step in report["next_steps"]:
            file_handle.write(f"{step}\n")
        file_handle.write("\n")
        
        file_handle.write("=" * 80 + "\n")
        file_handle.write("END OF REPORT\n")
        file_handle.write("=" * 80 + "\n")
    
    @staticmethod
    def generate_summary_report(all_reports: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate a summary report across multiple employee documents"""
        
        if not all_reports:
            return {"error": "No reports provided"}
        
        summary = {
            "summary_metadata": {
                "total_documents": len(all_reports),
                "report_timestamp": datetime.now().isoformat(),
                "report_version": "1.0"
            },
            "aggregate_statistics": {
                "total_scenarios": 0,
                "total_validations": 0,
                "total_passed": 0,
                "total_failed": 0,
                "total_critical_issues": 0,
                "average_success_rate": 0.0
            },
            "status_distribution": {},
            "common_issues": [],
            "recommendations": []
        }
        
        # Aggregate statistics
        success_rates = []
        status_counts = {}
        
        for report in all_reports:
            proc_summary = report["processing_summary"]
            summary["aggregate_statistics"]["total_scenarios"] += proc_summary["total_scenarios_processed"]
            summary["aggregate_statistics"]["total_validations"] += proc_summary["total_validations"]
            summary["aggregate_statistics"]["total_passed"] += proc_summary["passed_validations"]
            summary["aggregate_statistics"]["total_failed"] += proc_summary["failed_validations"]
            summary["aggregate_statistics"]["total_critical_issues"] += proc_summary["critical_issues"]
            
            success_rates.append(proc_summary["validation_success_rate"])
            
            status = proc_summary["overall_status"]
            status_counts[status] = status_counts.get(status, 0) + 1
        
        # Calculate averages
        if success_rates:
            summary["aggregate_statistics"]["average_success_rate"] = sum(success_rates) / len(success_rates)
        
        summary["status_distribution"] = status_counts
        
        return summary
