#!/usr/bin/env python3
"""
Logging configuration for the I-9 detection system.

This module provides a centralized logging configuration for the entire application.
"""

import logging
import sys
import os
from ..config import settings

def configure_logging():
    """
    Configure logging for the application.
    
    Sets up logging to both file and console with appropriate formatting.
    """
    # Create logger
    logger = logging.getLogger("hri9")
    logger.setLevel(getattr(logging, settings.LOG_LEVEL))
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Ensure log directory exists
    log_dir = os.path.dirname(settings.LOG_FILE)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)

    # Create file handler
    file_handler = logging.FileHandler(settings.LOG_FILE)
    file_handler.setFormatter(formatter)
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

# Create and export the logger instance
logger = configure_logging()
