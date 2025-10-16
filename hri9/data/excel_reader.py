#!/usr/bin/env python3
"""
Excel file processing for I-9 detection.

This module provides functionality for reading employee IDs from Excel files.
"""

import os
import re
import pandas as pd
from ..utils.logging_config import logger

class ExcelReader:
    """Class for reading and processing Excel files."""
    
    @staticmethod
    def read_employee_ids(excel_file):
        """
        Read employee IDs from an Excel file.
        
        Args:
            excel_file (str): Path to the Excel file.
            
        Returns:
            list: List of employee IDs.
        """
        try:
            if not os.path.exists(excel_file):
                logger.error(f"Excel file not found: {excel_file}")
                return []
                
            df = pd.read_excel(excel_file)
            if df.empty:
                logger.error("Excel file is empty")
                return []
                
            # The first column contains employee IDs, but we need to skip the header row
            id_column = df.columns[0]  # First column regardless of name
            
            # Skip the header row (index 0) and get employee IDs
            employee_ids = df.iloc[1:][id_column].astype(str).tolist()
            
            # Clean the IDs - remove any non-numeric characters and leading/trailing spaces
            employee_ids = [re.sub(r'\D', '', id.strip()) for id in employee_ids if id and id.strip()]
            
            # Remove duplicates while preserving order
            unique_ids = []
            seen = set()
            for id in employee_ids:
                if id not in seen and id:
                    seen.add(id)
                    unique_ids.append(id)
            
            logger.info(f"Found {len(unique_ids)} unique employee IDs in the Excel file")
            return unique_ids
        except Exception as e:
            logger.error(f"Error reading Excel file: {e}")
            return []
