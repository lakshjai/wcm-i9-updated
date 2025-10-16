#!/usr/bin/env python3
"""
File system operations for I-9 detection.

This module provides functionality for managing PDF files and directories.
"""

import os
import shutil
from pathlib import Path
from ..utils.logging_config import logger
from ..config import settings

class FileManager:
    """Class for managing file system operations."""
    
    @staticmethod
    def get_pdf_from_local_sample(employee_id, sample_dir=None):
        """
        Get PDF from local sample data.
        
        Args:
            employee_id (str): Employee ID.
            sample_dir (str, optional): Directory containing sample data.
                                      Defaults to settings.LOCAL_SAMPLE_PATH.
            
        Returns:
            str: Path to the PDF file, or None if not found.
        """
        try:
            sample_dir = Path(sample_dir or settings.LOCAL_SAMPLE_PATH)
            
            # Find folder that contains the employee ID
            matching_folders = [f for f in sample_dir.iterdir() if f.is_dir() and str(employee_id) in f.name]
            
            if not matching_folders:
                logger.warning(f"No sample folder found for employee ID {employee_id}")
                return None
            
            # Use the first matching folder
            employee_folder = matching_folders[0]
            
            # Find PDF files in the folder (case-insensitive)
            pdf_files = []
            for pattern in ["*.pdf", "*.PDF"]:
                pdf_files.extend(employee_folder.glob(pattern))
            
            if not pdf_files:
                logger.warning(f"No PDF files found in {employee_folder}")
                return None
            
            logger.info(f"Found sample PDF for employee {employee_id}: {pdf_files[0]}")
            return str(pdf_files[0])
        except Exception as e:
            logger.error(f"Error retrieving local sample PDF for employee {employee_id}: {e}")
            return None
    
    @staticmethod
    def get_pdf_from_network_drive(employee_id, network_dir=None, max_retries=None, retry_delay=None):
        """
        Get PDF from network drive with retry logic for network issues.
        
        Args:
            employee_id (str): Employee ID.
            network_dir (str, optional): Path to network drive.
                                       Defaults to settings.NETWORK_DRIVE_PATH.
            max_retries (int, optional): Maximum number of retry attempts.
                                       Defaults to settings.NETWORK_MAX_RETRIES.
            retry_delay (float, optional): Delay between retries in seconds.
                                         Defaults to settings.NETWORK_RETRY_DELAY.
            
        Returns:
            str: Path to the PDF file, or None if not found.
        """
        import time
        
        # Use settings defaults if not provided
        max_retries = max_retries if max_retries is not None else settings.NETWORK_MAX_RETRIES
        retry_delay = retry_delay if retry_delay is not None else settings.NETWORK_RETRY_DELAY
        
        for attempt in range(max_retries + 1):
            try:
                network_dir = Path(network_dir or settings.NETWORK_DRIVE_PATH)
                
                # Check if network drive is accessible with retry
                if not FileManager._check_network_drive_accessible(network_dir, attempt):
                    if attempt < max_retries:
                        logger.warning(f"Network drive not accessible, retrying in {retry_delay}s (attempt {attempt + 1}/{max_retries + 1})")
                        time.sleep(retry_delay)
                        continue
                    else:
                        logger.error(f"Network drive path not accessible after {max_retries + 1} attempts: {network_dir}")
                        return None
                
                # Find folder that contains the employee ID with retry
                matching_folders = FileManager._find_employee_folder_with_retry(network_dir, employee_id, attempt)
                
                if not matching_folders:
                    if attempt < max_retries:
                        logger.warning(f"No folder found for employee {employee_id}, retrying in {retry_delay}s (attempt {attempt + 1}/{max_retries + 1})")
                        time.sleep(retry_delay)
                        continue
                    else:
                        logger.warning(f"No folder found for employee ID {employee_id} in network drive after {max_retries + 1} attempts")
                        return None
                
                # Use the first matching folder
                employee_folder = matching_folders[0]
                
                # Find PDF files in the folder with retry
                pdf_files = FileManager._find_pdf_files_with_retry(employee_folder, attempt)
                
                if not pdf_files:
                    if attempt < max_retries:
                        logger.warning(f"No PDF files found in {employee_folder}, retrying in {retry_delay}s (attempt {attempt + 1}/{max_retries + 1})")
                        time.sleep(retry_delay)
                        continue
                    else:
                        logger.warning(f"No PDF files found in {employee_folder} after {max_retries + 1} attempts")
                        return None
                
                pdf_path = str(pdf_files[0])
                
                # Verify the file is actually accessible
                if FileManager._verify_file_accessible(pdf_path):
                    logger.info(f"Found PDF for employee {employee_id} in network drive: {pdf_path}")
                    return pdf_path
                else:
                    if attempt < max_retries:
                        logger.warning(f"PDF file not accessible, retrying in {retry_delay}s (attempt {attempt + 1}/{max_retries + 1})")
                        time.sleep(retry_delay)
                        continue
                    else:
                        logger.error(f"PDF file not accessible after {max_retries + 1} attempts: {pdf_path}")
                        return None
                        
            except Exception as e:
                if attempt < max_retries:
                    logger.warning(f"Error retrieving PDF for employee {employee_id} (attempt {attempt + 1}/{max_retries + 1}): {e}")
                    time.sleep(retry_delay)
                    continue
                else:
                    logger.error(f"Error retrieving PDF for employee {employee_id} from network drive after {max_retries + 1} attempts: {e}")
                    return None
        
        return None
    
    @staticmethod
    def _check_network_drive_accessible(network_dir, attempt_num):
        """Check if network drive is accessible."""
        try:
            return network_dir.exists() and network_dir.is_dir()
        except (OSError, PermissionError) as e:
            logger.debug(f"Network drive check failed (attempt {attempt_num + 1}): {e}")
            return False
    
    @staticmethod
    def _find_employee_folder_with_retry(network_dir, employee_id, attempt_num):
        """Find employee folder with error handling."""
        try:
            return [f for f in network_dir.iterdir() if f.is_dir() and str(employee_id) in f.name]
        except (OSError, PermissionError) as e:
            logger.debug(f"Employee folder search failed (attempt {attempt_num + 1}): {e}")
            return []
    
    @staticmethod
    def _find_pdf_files_with_retry(employee_folder, attempt_num):
        """Find PDF files with error handling."""
        try:
            pdf_files = []
            for pattern in ["*.pdf", "*.PDF"]:
                pdf_files.extend(employee_folder.glob(pattern))
            return pdf_files
        except (OSError, PermissionError) as e:
            logger.debug(f"PDF file search failed (attempt {attempt_num + 1}): {e}")
            return []
    
    @staticmethod
    def _verify_file_accessible(file_path):
        """Verify that a file is actually accessible."""
        try:
            # Try to open the file briefly to verify it's accessible
            with open(file_path, 'rb') as f:
                f.read(1)  # Read just 1 byte to verify access
            return True
        except (OSError, PermissionError, FileNotFoundError) as e:
            logger.debug(f"File accessibility check failed for {file_path}: {e}")
            return False
    
    @staticmethod
    def ensure_directory_exists(directory_path):
        """
        Ensure a directory exists, creating it if necessary.
        
        Args:
            directory_path (str): Path to the directory.
            
        Returns:
            bool: True if directory exists or was created, False otherwise.
        """
        try:
            os.makedirs(directory_path, exist_ok=True)
            return True
        except Exception as e:
            logger.error(f"Error creating directory {directory_path}: {e}")
            return False
    
    @staticmethod
    def copy_file(source_path, destination_path):
        """
        Copy a file from source to destination.
        
        Args:
            source_path (str): Path to the source file.
            destination_path (str): Path to the destination file.
            
        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            # Ensure destination directory exists
            os.makedirs(os.path.dirname(destination_path), exist_ok=True)
            
            # Copy the file
            shutil.copy2(source_path, destination_path)
            logger.info(f"Copied {source_path} to {destination_path}")
            return True
        except Exception as e:
            logger.error(f"Error copying file {source_path} to {destination_path}: {e}")
            return False
