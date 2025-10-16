#!/usr/bin/env python3
"""
Reporting utilities for I-9 detection.

This module provides functionality for generating reports and statistics.
"""

import os
import csv
import time
from ..utils.logging_config import logger

class Reporter:
    """Class for generating reports and statistics."""
    
    @staticmethod
    def initialize_csv(csv_path, headers=None):
        """
        Initialize a CSV file with headers.
        
        Args:
            csv_path (str): Path to the CSV file.
            headers (list, optional): List of column headers.
                                    Defaults to standard I-9 detection headers.
            
        Returns:
            tuple: (csv_file, csv_writer) or (None, None) if error.
        """
        try:
            if headers is None:
                headers = ['Employee ID', 'PDF File Name', 'I-9 Forms Found', 
                          'Pages Removed', 'Success', 'Extracted I-9 Path']
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(csv_path), exist_ok=True)
            
            # Open file and initialize writer
            csv_file = open(csv_path, 'w', newline='', encoding='utf-8')
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(headers)
            csv_file.flush()
            
            logger.info(f"Initialized CSV report at {csv_path}")
            return csv_file, csv_writer
        except Exception as e:
            logger.error(f"Error initializing CSV report: {e}")
            return None, None
    
    @staticmethod
    def write_csv_row(csv_writer, csv_file, row_data):
        """
        Write a row to a CSV file.
        
        Args:
            csv_writer: CSV writer object.
            csv_file: CSV file object.
            row_data (list): Row data to write.
            
        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            csv_writer.writerow(row_data)
            csv_file.flush()
            return True
        except Exception as e:
            logger.error(f"Error writing to CSV: {e}")
            return False
    
    @staticmethod
    def generate_summary(processed, found_i9, removed_i9, extracted_i9, elapsed_time):
        """
        Generate a summary of the I-9 detection process.
        
        Args:
            processed (int): Number of documents processed.
            found_i9 (int): Number of I-9 forms found.
            removed_i9 (int): Number of I-9 forms removed.
            extracted_i9 (int): Number of I-9 forms extracted.
            elapsed_time (float): Elapsed time in seconds.
            
        Returns:
            str: Summary text.
        """
        rate = processed / elapsed_time if elapsed_time > 0 else 0
        
        summary = [
            f"I-9 Detection Summary",
            f"--------------------",
            f"Documents processed: {processed}",
            f"I-9 forms found: {found_i9}",
            f"I-9 forms removed: {removed_i9}",
            f"I-9 forms extracted: {extracted_i9}",
            f"Processing time: {elapsed_time:.1f} seconds",
            f"Processing rate: {rate:.2f} docs/sec"
        ]
        
        if processed > 0:
            summary.append(f"I-9 detection rate: {found_i9/processed*100:.1f}%")
            summary.append(f"Success rate: {removed_i9/processed*100:.1f}%")
        
        return "\n".join(summary)
    
    @staticmethod
    def log_progress(processed, total, found_i9, removed_i9, extracted_i9, 
                    start_time, last_report_time, last_report_count, batch_size):
        """
        Log progress of the I-9 detection process.
        
        Args:
            processed (int): Number of documents processed.
            total (int): Total number of documents to process.
            found_i9 (int): Number of I-9 forms found.
            removed_i9 (int): Number of I-9 forms removed.
            extracted_i9 (int): Number of I-9 forms extracted.
            start_time (float): Start time of the process.
            last_report_time (float): Time of the last progress report.
            last_report_count (int): Document count at the last progress report.
            batch_size (int): Progress reporting interval.
            
        Returns:
            tuple: (current_time, processed) for the next progress report.
        """
        if processed % batch_size == 0 or processed == total:
            current_time = time.time()
            elapsed = current_time - start_time
            interval = current_time - last_report_time
            docs_since_last = processed - last_report_count
            
            if interval > 0 and docs_since_last > 0:
                rate = docs_since_last / interval
                eta = (total - processed) / rate if rate > 0 else 0
                eta_str = f"{eta:.1f} seconds" if eta < 60 else f"{eta/60:.1f} minutes"
                
                logger.info(f"Progress: {processed}/{total} ({processed/total*100:.1f}%) | "
                          f"Found: {found_i9} | Removed: {removed_i9} | Extracted: {extracted_i9} | "
                          f"Rate: {rate:.2f} docs/sec | ETA: {eta_str}")
                
                return current_time, processed
        
        return last_report_time, last_report_count
        
    @staticmethod
    def write_deletion_record(csv_path, employee_id, employee_name, file_path):
        """
        Write a record to the deletion CSV file.
        
        Args:
            csv_path (str): Path to the deletion CSV file.
            employee_id (str): Employee ID.
            employee_name (str): Employee name extracted from folder name.
            file_path (str): Absolute path to the file to be deleted.
            
        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(csv_path), exist_ok=True)
            
            # Check if file exists to determine if we need to write headers
            file_exists = os.path.isfile(csv_path)
            
            # Open file in append mode
            with open(csv_path, 'a', newline='', encoding='utf-8') as csv_file:
                csv_writer = csv.writer(csv_file)
                
                # Write headers if file is new
                if not file_exists:
                    csv_writer.writerow(['Employee ID', 'Employee Name', 'File Path', 'Timestamp'])
                
                # Write the deletion record
                timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
                csv_writer.writerow([employee_id, employee_name, file_path, timestamp])
            
            logger.info(f"Recorded file for deletion: {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error writing deletion record: {e}")
            return False
