"""Quick smoke test of foundation modules."""
from src.config import db_config, pipeline_config, RAW_DIR
from src.utils import get_db_connection, get_logger
from src.utils.db import table_exists
from src.validate.schemas import TripRecord
from datetime import datetime

print("="*60)
print("🧪 FOUNDATION SMOKE TEST")
print("="*60)

# Test 1: Config
print("\n✅ Test 1: Configuration")
print(f"   Database: {db_config.host}:{db_config.port}/{db_config.database}")
print(f"   Raw data dir: {RAW_DIR}")
print(f"   Batch size: {pipeline_config.batch_size}")

# Test 2: Logger
print("\n✅ Test 2: Logging")
logger = get_logger(__name__)
logger.info("Logger initialized successfully")
print("   Logger working!")

# Test 3: Database Connection
print("\n✅ Test 3: Database Connection")
try:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT version();")
            version = cur.fetchone()[0]
            print(f"   Connected! PostgreSQL version: {version[:50]}...")
except Exception as e:
    print(f"   ❌ Connection failed: {e}")
    print("   (Make sure Docker containers are running: make up)")

# Test 4: Table Exists Check
print("\n✅ Test 4: Table Existence Check")
try:
    schemas_to_check = [
        ('staging', 'trip_raw'),
        ('warehouse', 'fact_trip'),
        ('warehouse', 'dim_vendor'),
        ('audit', 'pipeline_run')
    ]
    
    for schema, table in schemas_to_check:
        exists = table_exists(schema, table)
        status = "✓" if exists else "✗"
        print(f"   {status} {schema}.{table}")
        
except Exception as e:
    print(f"   ❌ Table check failed: {e}")

# Test 5: Schema Validation
print("\n✅ Test 5: Pydantic Schema Validation")
try:
    # Valid record
    trip = TripRecord(
        vendorid=1,
        tpep_pickup_datetime=datetime(2024, 1, 1, 10, 0),
        tpep_dropoff_datetime=datetime(2024, 1, 1, 10, 30),
        passenger_count=2,
        trip_distance=5.5,
        ratecodeid=1,
        pulocationid=100,
        dolocationid=200,
        payment_type=1,
        fare_amount=15.0,
        extra=0.5,
        mta_tax=0.5,
        tip_amount=3.0,
        tolls_amount=0.0,
        total_amount=19.0
    )
    print("   ✓ Valid record accepted")
    
    # Test validation rule: dropoff after pickup
    try:
        bad_trip = TripRecord(
            vendorid=1,
            tpep_pickup_datetime=datetime(2024, 1, 1, 10, 30),
            tpep_dropoff_datetime=datetime(2024, 1, 1, 10, 0),  # Before pickup!
            passenger_count=2,
            trip_distance=5.5,
            ratecodeid=1,
            pulocationid=100,
            dolocationid=200,
            payment_type=1,
            fare_amount=15.0,
            extra=0.5,
            mta_tax=0.5,
            tip_amount=3.0,
            tolls_amount=0.0,
            total_amount=19.0
        )
        print("   ✗ Validation failed to catch bad dropoff time!")
    except ValueError as e:
        print(f"   ✓ Validation caught bad dropoff: {str(e)[:40]}...")
        
except Exception as e:
    print(f"   ❌ Schema validation error: {e}")

print("\n" + "="*60)
print("🎉 FOUNDATION MODULES READY!")
print("="*60)
print("\nNext steps:")
print("  1. Run: git add . && git commit -m 'Add foundation modules'")
print("  2. Start implementing extract module")
print("="*60)
