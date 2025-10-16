#!/usr/bin/env python3
"""
File filtering utilities for debug mode and selective processing.

This module provides functionality for filtering employee files based on patterns
and other criteria for debugging and selective processing.
"""

import os
from pathlib import Path
from typing import List, Optional
from ..utils.logging_config import logger
from ..data.file_manager import FileManager

class FileFilter:
    """Utility class for filtering files based on various criteria."""
    
    @staticmethod
    def filter_employees_by_pattern(employee_ids: List[str], pattern: str, 
                                   use_local: bool = False) -> List[str]:
        """
        Filter employee IDs based on a pattern match in their file paths or names.
        
        Args:
            employee_ids (List[str]): List of employee IDs to filter.
            pattern (str): Pattern to search for in file paths/names.
            use_local (bool): Whether to use local sample data.
            
        Returns:
            List[str]: Filtered list of employee IDs that match the pattern.
        """
        if not pattern:
            return employee_ids
        
        logger.info(f"Filtering employees by pattern: '{pattern}'")
        
        filtered_ids = []
        pattern_lower = pattern.lower()
        
        for employee_id in employee_ids:
            try:
                # Get the PDF path for this employee
                if use_local:
                    pdf_path = FileManager.get_pdf_from_local_sample(employee_id)
                else:
                    pdf_path = FileManager.get_pdf_from_network_drive(employee_id)
                
                if not pdf_path:
                    continue
                
                # Check if pattern matches in various parts of the path
                matches = [
                    pattern_lower in employee_id.lower(),
                    pattern_lower in os.path.basename(pdf_path).lower(),
                    pattern_lower in os.path.dirname(pdf_path).lower(),
                    pattern_lower in pdf_path.lower()
                ]
                
                if any(matches):
                    filtered_ids.append(employee_id)
                    logger.debug(f"Employee {employee_id} matches pattern (file: {os.path.basename(pdf_path)})")
                
            except Exception as e:
                logger.warning(f"Error checking pattern for employee {employee_id}: {e}")
                continue
        
        logger.info(f"Pattern filter '{pattern}' matched {len(filtered_ids)} out of {len(employee_ids)} employees")
        
        if filtered_ids:
            logger.info(f"Matched employees: {filtered_ids[:10]}{'...' if len(filtered_ids) > 10 else ''}")
        else:
            logger.warning(f"No employees matched pattern '{pattern}'. Available employees: {employee_ids[:5]}{'...' if len(employee_ids) > 5 else ''}")
        
        return filtered_ids
    
    @staticmethod
    def check_existing_catalog_files(employee_id: str, catalog_output_dir: str, 
                                   use_local: bool = False) -> dict:
        """
        Check if catalog files already exist for an employee.
        
        Args:
            employee_id (str): Employee ID to check.
            catalog_output_dir (str): Directory where catalog files are stored.
            use_local (bool): Whether to use local sample data.
            
        Returns:
            dict: Dictionary with existence status of catalog files.
        """
        try:
            # Get the PDF path to determine the base filename
            if use_local:
                pdf_path = FileManager.get_pdf_from_local_sample(employee_id)
            else:
                pdf_path = FileManager.get_pdf_from_network_drive(employee_id)
            
            if not pdf_path:
                return {'exists': False, 'text_path': None, 'json_path': None}
            
            # Generate expected catalog filenames
            original_filename = Path(pdf_path).stem
            text_filename = f"{original_filename}.catalog.txt"
            json_filename = f"{original_filename}.catalog.json"
            
            text_path = os.path.join(catalog_output_dir, text_filename)
            json_path = os.path.join(catalog_output_dir, json_filename)
            
            text_exists = os.path.exists(text_path)
            json_exists = os.path.exists(json_path)
            
            return {
                'exists': text_exists or json_exists,
                'text_exists': text_exists,
                'json_exists': json_exists,
                'text_path': text_path if text_exists else None,
                'json_path': json_path if json_exists else None,
                'both_exist': text_exists and json_exists
            }
            
        except Exception as e:
            logger.error(f"Error checking existing catalog files for {employee_id}: {e}")
            return {'exists': False, 'text_path': None, 'json_path': None}
    
    @staticmethod
    def filter_employees_by_existing_catalogs(employee_ids: List[str], catalog_output_dir: str,
                                            skip_existing: bool = True, use_local: bool = False) -> List[str]:
        """
        Filter employee IDs based on existing catalog files.
        
        Args:
            employee_ids (List[str]): List of employee IDs to filter.
            catalog_output_dir (str): Directory where catalog files are stored.
            skip_existing (bool): If True, skip employees with existing catalogs.
                                If False, only process employees with existing catalogs.
            use_local (bool): Whether to use local sample data.
            
        Returns:
            List[str]: Filtered list of employee IDs.
        """
        if not catalog_output_dir:
            return employee_ids
        
        logger.info(f"Filtering employees by existing catalogs (skip_existing={skip_existing})")
        
        filtered_ids = []
        existing_count = 0
        
        for employee_id in employee_ids:
            catalog_status = FileFilter.check_existing_catalog_files(
                employee_id, catalog_output_dir, use_local
            )
            
            has_catalog = catalog_status['exists']
            
            if has_catalog:
                existing_count += 1
            
            # Include employee based on skip_existing setting
            if skip_existing and not has_catalog:
                filtered_ids.append(employee_id)
            elif not skip_existing and has_catalog:
                filtered_ids.append(employee_id)
        
        action = "skipping" if skip_existing else "processing only"
        logger.info(f"Found {existing_count} employees with existing catalogs, {action} them")
        logger.info(f"Filtered to {len(filtered_ids)} employees for processing")
        
        return filtered_ids
    
    @staticmethod
    def get_catalog_file_paths(employee_id: str, catalog_output_dir: str, 
                              use_local: bool = False) -> dict:
        """
        Get the expected catalog file paths for an employee.
        
        Args:
            employee_id (str): Employee ID.
            catalog_output_dir (str): Directory where catalog files are stored.
            use_local (bool): Whether to use local sample data.
            
        Returns:
            dict: Dictionary with catalog file paths.
        """
        try:
            # Get the PDF path to determine the base filename
            if use_local:
                pdf_path = FileManager.get_pdf_from_local_sample(employee_id)
            else:
                pdf_path = FileManager.get_pdf_from_network_drive(employee_id)
            
            if not pdf_path:
                return {'text_path': None, 'json_path': None}
            
            # Generate catalog filenames
            original_filename = Path(pdf_path).stem
            text_filename = f"{original_filename}.catalog.txt"
            json_filename = f"{original_filename}.catalog.json"
            
            text_path = os.path.join(catalog_output_dir, text_filename)
            json_path = os.path.join(catalog_output_dir, json_filename)
            
            return {
                'text_path': text_path,
                'json_path': json_path,
                'text_filename': text_filename,
                'json_filename': json_filename
            }
            
        except Exception as e:
            logger.error(f"Error getting catalog file paths for {employee_id}: {e}")
            return {'text_path': None, 'json_path': None}
    
    @staticmethod
    def validate_debug_pattern(pattern: str, employee_ids: List[str], 
                              use_local: bool = False) -> dict:
        """
        Validate a debug pattern and provide feedback on matches.
        
        Args:
            pattern (str): Pattern to validate.
            employee_ids (List[str]): List of employee IDs to check against.
            use_local (bool): Whether to use local sample data.
            
        Returns:
            dict: Validation results with match information.
        """
        if not pattern:
            return {
                'valid': False,
                'error': 'Pattern cannot be empty',
                'matches': 0,
                'suggestions': []
            }
        
        # Get matches
        matches = FileFilter.filter_employees_by_pattern(employee_ids, pattern, use_local)
        
        result = {
            'valid': len(matches) > 0,
            'matches': len(matches),
            'matched_ids': matches[:10],  # First 10 matches
            'total_employees': len(employee_ids)
        }
        
        if len(matches) == 0:
            # Provide suggestions for similar patterns
            suggestions = []
            pattern_lower = pattern.lower()
            
            # Check for partial matches in employee IDs
            for emp_id in employee_ids[:20]:  # Check first 20 for suggestions
                if any(char in emp_id.lower() for char in pattern_lower):
                    suggestions.append(emp_id)
            
            result['error'] = f'No employees match pattern "{pattern}"'
            result['suggestions'] = suggestions[:5]  # Top 5 suggestions
        
        return result