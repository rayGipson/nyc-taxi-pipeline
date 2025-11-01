"""Simple downloader - no logging, minimal error handling."""
from pathlib import Path
import requests


def download_taxi_data_simple(year: int, month: int, output_dir: Path) -> Path:
    """
    Download NYC Taxi data - simple version.
    
    This is the "I'd write this from scratch" version.
    No logging, basic error handling, works but not production-ready.
    """
    # Validate inputs
    if not 1 <= month <= 12:
        raise ValueError(f"Month must be 1-12, got {month}")
    
    # Build URL and path
    filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
    output_path = output_dir / filename
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}"
    
    # Skip if exists
    if output_path.exists():
        print(f"File exists, skipping: {output_path}")
        return output_path
    
    # Create directory
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Download
    response = requests.get(url, timeout=60)
    
    if response.status_code != 200:
        raise Exception(f"Download failed: HTTP {response.status_code}")
    
    # Write file
    with open(output_path, 'wb') as f:
        f.write(response.content)
    
    print(f"Downloaded: {output_path}")
    return output_path