import argparse
import logging
import time
import sys
import os
import signal
import json
from datetime import datetime
from typing import Dict, Any, Optional
import importlib.util

class ColoredFormatter(logging.Formatter):
    """Custom formatter that adds colors to log levels"""
    
    # ANSI color codes
    COLORS = {
        'DEBUG': '\033[36m',    # cyan
        'INFO': '\033[32m',     # green
        'WARNING': '\033[33m',  # yellow
        'ERROR': '\033[31m',    # red
        'CRITICAL': '\033[35m', # magenta
    }
    RESET = '\033[0m'
    BOLD = '\033[1m'
    
    def __init__(self, use_colors=True):
        super().__init__()
        self.use_colors = use_colors and self._supports_color()
    
    def _supports_color(self):
        """Check if terminal supports colors"""
        return (
            hasattr(sys.stderr, "isatty") and sys.stderr.isatty() and
            os.environ.get('TERM') != 'dumb' and
            os.environ.get('NO_COLOR') is None
        )
    
    def format(self, record):
        if self.use_colors:
            level_color = self.COLORS.get(record.levelname, '')
            level_name = f"{level_color}{self.BOLD}{record.levelname:<8}{self.RESET}"
            
            # format timestamp with subdued color
            timestamp = f"\033[90m{self.formatTime(record, '%H:%M:%S')}\033[0m"
            
            return f"{timestamp} {level_name} {record.getMessage()}"
        else:
            # fallback to standard format without colors
            return f"{self.formatTime(record, '%Y-%m-%d %H:%M:%S')} - {record.levelname} - {record.getMessage()}"

# configure colored logging
def setup_logging(verbose=False, no_color=False):
    """Setup logging with colors and appropriate level"""
    logger = logging.getLogger()
    
    # remove existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # create console handler with colored formatter
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setFormatter(ColoredFormatter(use_colors=not no_color))
    
    # set level
    level = logging.DEBUG if verbose else logging.INFO
    logger.setLevel(level)
    console_handler.setLevel(level)
    
    logger.addHandler(console_handler)
    
    return logger

# initial setup with INFO level
logger = setup_logging()