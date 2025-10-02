"""Structured logging configuration for the pipeline."""
import logging
import logging.config
import sys
from pathlib import Path
from typing import Optional
import yaml

from src.config import PROJECT_ROOT


def setup_logging(config_path: Optional[Path] = None, log_level: Optional[str] = None) -> None:
    """
    Configure logging from YAML config file.
    
    Falls back to basic config if YAML file not found.
    
    Args:
        config_path: Path to logging.yaml config file
        log_level: Override log level (DEBUG, INFO, WARNING, ERROR)
    """
    if config_path is None:
        config_path = PROJECT_ROOT / "config" / "logging.yaml"
    
    # Try to load YAML config
    if config_path.exists():
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
                logging.config.dictConfig(config)
            
            # Override log level if specified
            if log_level:
                logging.getLogger().setLevel(log_level.upper())
                
        except Exception as e:
            # Fall back to basic config if YAML parsing fails
            _setup_basic_logging(log_level)
            logging.warning(f"Failed to load logging config from {config_path}: {e}")
    else:
        _setup_basic_logging(log_level)
        logging.warning(f"Logging config not found at {config_path}, using basic config")


def _setup_basic_logging(log_level: Optional[str] = None) -> None:
    """
    Configure basic logging as fallback.
    
    Logs to both console and file with consistent formatting.
    """
    level = getattr(logging, log_level.upper()) if log_level else logging.INFO
    
    # Create formatter
    formatter = logging.Formatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    
    # File handler (logs directory)
    log_dir = PROJECT_ROOT / "logs"
    log_dir.mkdir(exist_ok=True)
    file_handler = logging.FileHandler(log_dir / "pipeline.log")
    file_handler.setFormatter(formatter)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance for a module.
    
    Args:
        name: Logger name (typically __name__ of the calling module)
        
    Returns:
        Configured logger instance
        
    Example:
        >>> logger = get_logger(__name__)
        >>> logger.info("Processing started")
    """
    return logging.getLogger(name)


class LogContext:
    """
    Context manager for adding temporary context to log messages.
    
    Example:
        >>> logger = get_logger(__name__)
        >>> with LogContext(logger, run_id="20240115_001", stage="extract"):
        ...     logger.info("Processing file")
        # Output: ... - extract - run_id=20240115_001 - Processing file
    """
    
    def __init__(self, logger: logging.Logger, **context):
        """
        Initialize log context.
        
        Args:
            logger: Logger instance to add context to
            **context: Key-value pairs to add to log messages
        """
        self.logger = logger
        self.context = context
        self.old_factory = None
    
    def __enter__(self):
        """Add context to logger on entry."""
        self.old_factory = logging.getLogRecordFactory()
        
        def record_factory(*args, **kwargs):
            record = self.old_factory(*args, **kwargs)
            # Add context to the message
            context_str = " - ".join(f"{k}={v}" for k, v in self.context.items())
            record.msg = f"{context_str} - {record.msg}"
            return record
        
        logging.setLogRecordFactory(record_factory)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Restore original log factory on exit."""
        logging.setLogRecordFactory(self.old_factory)


# Initialize logging on module import
setup_logging()
