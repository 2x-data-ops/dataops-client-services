import logging
import os
import glob
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler

def setup_logging():
    """Set up logging with rotation and cleanup of old logs."""
    log_dir = 'logs'
    os.makedirs(log_dir, exist_ok=True)
    
    # Clean up old logs if more than 10 exist
    def cleanup_old_logs():
        log_files = glob.glob(os.path.join(log_dir, 'data_pipeline_*.log'))
        if len(log_files) > 10:
            # Sort files by modification time (oldest first)
            log_files.sort(key=os.path.getmtime)
            # Remove oldest files until only 10 remain
            for file in log_files[:-10]:
                try:
                    os.remove(file)
                    print(f"Removed old log file: {file}")
                except Exception as e:
                    print(f"Error removing log file {file}: {e}")

    # Clean up old logs before creating new one
    cleanup_old_logs()

    # Create new log file with timestamp
    current_time = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = os.path.join(log_dir, f'data_pipeline_{current_time}.log')
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-8s | %(filename)s:%(lineno)d | %(funcName)s | %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    return logging.getLogger(__name__)