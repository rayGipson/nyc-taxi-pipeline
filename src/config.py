"""Configuration management for the pipeline."""
import os
from pathlib import Path
from typing import Optional
from pydantic import BaseModel, Field
from dotenv import load_dotenv

load_dotenv()

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
STAGING_DIR = DATA_DIR / "staging"
REJECTED_DIR = DATA_DIR / "rejected"

# Ensure directories exist
for dir_path in [RAW_DIR, STAGING_DIR, REJECTED_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)


# Helper function for safe integer parsing
def _get_env_int(key: str, default: int) -> int:
    """Safely parse integer from environment variable."""
    try:
        return int(os.getenv(key, str(default)))
    except ValueError:
        return default


class DatabaseConfig(BaseModel):
    """Database connection configuration."""
    host: str = Field(default_factory=lambda: os.getenv("POSTGRES_HOST", "localhost"))
    port: int = Field(default_factory=lambda: _get_env_int("POSTGRES_PORT", 5432))
    user: str = Field(default_factory=lambda: os.getenv("POSTGRES_USER", "dataeng"))
    password: str = Field(default_factory=lambda: os.getenv("POSTGRES_PASSWORD", ""))
    database: str = Field(default_factory=lambda: os.getenv("POSTGRES_DB", "taxi_analytics"))
    
    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


class PipelineConfig(BaseModel):
    """Pipeline execution configuration."""
    batch_size: int = 10000
    max_rejected_pct: float = 5.0  # Fail if >5% rows rejected
    enable_profiling: bool = True
    

# Global config instances
db_config = DatabaseConfig()
pipeline_config = PipelineConfig()
