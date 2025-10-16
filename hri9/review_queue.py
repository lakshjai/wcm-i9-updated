#!/usr/bin/env python3

import json
import csv
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any
from dataclasses import dataclass

from .utils.logging_config import logger


@dataclass
class ReviewItem:
    """Item that needs manual review"""
    filename: str
    reason: str
    confidence_score: float
    validation_score: float
    issues: List[str]
    priority: str  # HIGH, MEDIUM, LOW
    timestamp: str


class ReviewQueueManager:
    """Manages files that need manual review due to low confidence or processing issues"""
    
    def __init__(self, output_dir: str = "workdir/review_queue"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Thresholds for review queue
        self.LOW_CONFIDENCE_THRESHOLD = 0.7  # Below 70% confidence
        self.LOW_VALIDATION_THRESHOLD = 60.0  # Below 60% validation score
        self.CRITICAL_ISSUES = [
            "Missing Section 1",
            "Missing Section 2", 
            "Processing failed",
            "JSON parsing error",
            "Document extraction failed"
        ]
        
        self.review_items = []
    
    def evaluate_for_review(self, filename: str, processing_result: Dict) -> bool:
        """
        Evaluate if a file needs manual review
        
        Args:
            filename: Name of the processed file
            processing_result: Processing result data
            
        Returns:
            bool: True if file needs review
        """
        needs_review = False
        reasons = []
        priority = "LOW"
        
        # Extract scores and validation data
        validation_score = self._extract_validation_score(processing_result)
        confidence_score = self._extract_confidence_score(processing_result)
        validation_issues = self._extract_validation_issues(processing_result)
        
        # Check confidence score
        if confidence_score < self.LOW_CONFIDENCE_THRESHOLD:
            needs_review = True
            reasons.append(f"Low confidence score: {confidence_score:.1%}")
            priority = "MEDIUM"
        
        # Check validation score
        if validation_score < self.LOW_VALIDATION_THRESHOLD:
            needs_review = True
            reasons.append(f"Low validation score: {validation_score:.1%}")
            if validation_score < 40.0:
                priority = "HIGH"
        
        # Check for critical issues
        critical_issues_found = []
        for issue in validation_issues:
            for critical_pattern in self.CRITICAL_ISSUES:
                if critical_pattern.lower() in issue.lower():
                    critical_issues_found.append(issue)
                    needs_review = True
                    priority = "HIGH"
        
        # Check processing status
        processing_status = processing_result.get('processing_status', '')
        if processing_status in ['ERROR', 'PARTIAL_SUCCESS']:
            needs_review = True
            reasons.append(f"Processing status: {processing_status}")
            if processing_status == 'ERROR':
                priority = "HIGH"
        
        # Check I-9 set completeness
        i9_set_type = processing_result.get('i9_set_type', '')
        if i9_set_type == 'INCOMPLETE':
            needs_review = True
            reasons.append("I-9 set incomplete")
        
        # Check document matching
        doc_matching_score = self._extract_document_matching_score(processing_result)
        if doc_matching_score < 50.0:
            needs_review = True
            reasons.append(f"Poor document matching: {doc_matching_score:.1%}")
        
        if needs_review:
            review_item = ReviewItem(
                filename=filename,
                reason=" | ".join(reasons),
                confidence_score=confidence_score,
                validation_score=validation_score,
                issues=validation_issues,
                priority=priority,
                timestamp=datetime.now().isoformat()
            )
            self.review_items.append(review_item)
            
            logger.warning(f"📋 REVIEW QUEUE: {filename} - {priority} priority - {review_item.reason}")
        
        return needs_review
    
    def _extract_validation_score(self, result: Dict) -> float:
        """Extract validation score from processing result"""
        try:
            score_str = result.get('validation_score', '0%')
            if isinstance(score_str, str) and '%' in score_str:
                return float(score_str.replace('%', ''))
            elif isinstance(score_str, (int, float)):
                return float(score_str)
            return 0.0
        except:
            return 0.0
    
    def _extract_confidence_score(self, result: Dict) -> float:
        """Extract confidence score from processing result"""
        try:
            # Look for confidence in various places
            confidence = result.get('confidence_score', 0.0)
            if isinstance(confidence, str) and '%' in confidence:
                return float(confidence.replace('%', '')) / 100.0
            elif isinstance(confidence, (int, float)):
                return float(confidence) if confidence <= 1.0 else confidence / 100.0
            return 0.0
        except:
            return 0.0
    
    def _extract_validation_issues(self, result: Dict) -> List[str]:
        """Extract validation issues from processing result"""
        issues = []
        
        # Check validation_issues field
        validation_issues = result.get('validation_issues', [])
        if isinstance(validation_issues, list):
            issues.extend(validation_issues)
        elif isinstance(validation_issues, str):
            issues.append(validation_issues)
        
        # Check validation_errors field
        validation_errors = result.get('validation_errors', [])
        if isinstance(validation_errors, list):
            issues.extend(validation_errors)
        elif isinstance(validation_errors, str):
            issues.append(validation_errors)
        
        return issues
    
    def _extract_document_matching_score(self, result: Dict) -> float:
        """Extract document matching score from validation rules"""
        try:
            rules_applied = result.get('validation_rules_applied', '')
            if 'Document Matching:' in rules_applied:
                # Extract "Document Matching: XX.X%"
                import re
                match = re.search(r'Document Matching:\s*([\d.]+)%', rules_applied)
                if match:
                    return float(match.group(1))
            return 0.0
        except:
            return 0.0
    
    def generate_review_queue_report(self) -> str:
        """Generate comprehensive review queue report"""
        
        if not self.review_items:
            logger.info("✅ No files need manual review - all processing successful!")
            return ""
        
        # Sort by priority (HIGH -> MEDIUM -> LOW) and then by validation score (lowest first)
        priority_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
        sorted_items = sorted(
            self.review_items, 
            key=lambda x: (priority_order[x.priority], x.validation_score)
        )
        
        # Generate CSV report
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_filename = f"review_queue_{timestamp}.csv"
        csv_path = self.output_dir / csv_filename
        
        with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # Header
            writer.writerow([
                'Priority', 'Filename', 'Validation Score', 'Confidence Score',
                'Review Reason', 'Issues Found', 'Timestamp'
            ])
            
            # Data rows
            for item in sorted_items:
                writer.writerow([
                    item.priority,
                    item.filename,
                    f"{item.validation_score:.1f}%",
                    f"{item.confidence_score:.1%}",
                    item.reason,
                    " | ".join(item.issues[:3]) + ("..." if len(item.issues) > 3 else ""),
                    item.timestamp
                ])
        
        # Generate summary report
        summary_filename = f"review_queue_summary_{timestamp}.txt"
        summary_path = self.output_dir / summary_filename
        
        with open(summary_path, 'w', encoding='utf-8') as f:
            f.write("=" * 80 + "\n")
            f.write("I-9 PROCESSING REVIEW QUEUE SUMMARY\n")
            f.write("=" * 80 + "\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            # Statistics
            high_priority = len([item for item in sorted_items if item.priority == "HIGH"])
            medium_priority = len([item for item in sorted_items if item.priority == "MEDIUM"])
            low_priority = len([item for item in sorted_items if item.priority == "LOW"])
            
            f.write(f"REVIEW QUEUE STATISTICS:\n")
            f.write(f"  Total files needing review: {len(sorted_items)}\n")
            f.write(f"  High priority: {high_priority}\n")
            f.write(f"  Medium priority: {medium_priority}\n")
            f.write(f"  Low priority: {low_priority}\n\n")
            
            # Top issues
            f.write("TOP REVIEW REASONS:\n")
            reason_counts = {}
            for item in sorted_items:
                for reason in item.reason.split(" | "):
                    reason_counts[reason] = reason_counts.get(reason, 0) + 1
            
            for reason, count in sorted(reason_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
                f.write(f"  {reason}: {count} files\n")
            
            f.write("\n" + "=" * 80 + "\n")
            f.write("HIGH PRIORITY FILES (IMMEDIATE ATTENTION REQUIRED):\n")
            f.write("=" * 80 + "\n")
            
            for item in sorted_items:
                if item.priority == "HIGH":
                    f.write(f"\n📋 {item.filename}\n")
                    f.write(f"   Validation Score: {item.validation_score:.1f}%\n")
                    f.write(f"   Confidence Score: {item.confidence_score:.1%}\n")
                    f.write(f"   Reason: {item.reason}\n")
                    if item.issues:
                        f.write(f"   Issues: {', '.join(item.issues[:2])}\n")
        
        logger.info(f"📋 Review queue report generated:")
        logger.info(f"   📄 CSV Report: {csv_path}")
        logger.info(f"   📄 Summary: {summary_path}")
        logger.info(f"   🚨 {high_priority} HIGH priority files need immediate attention")
        logger.info(f"   ⚠️ {medium_priority} MEDIUM priority files need review")
        logger.info(f"   ℹ️ {low_priority} LOW priority files for quality check")
        
        return str(csv_path)
    
    def get_review_summary(self) -> Dict[str, Any]:
        """Get summary statistics for review queue"""
        if not self.review_items:
            return {
                "total_files": 0,
                "high_priority": 0,
                "medium_priority": 0,
                "low_priority": 0,
                "needs_review": False
            }
        
        high_priority = len([item for item in self.review_items if item.priority == "HIGH"])
        medium_priority = len([item for item in self.review_items if item.priority == "MEDIUM"])
        low_priority = len([item for item in self.review_items if item.priority == "LOW"])
        
        return {
            "total_files": len(self.review_items),
            "high_priority": high_priority,
            "medium_priority": medium_priority,
            "low_priority": low_priority,
            "needs_review": True
        }
