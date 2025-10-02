"""Data contracts and validation schemas."""
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field, field_validator, ConfigDict

class TripRecord(BaseModel):
    """Expected schema for NYC taxi trip records."""
    
    model_config = ConfigDict(
        str_strip_whitespace=True,  # Auto-strip whitespace
        validate_assignment=True,    # Validate on attribute assignment
    )

    vendorid: int = Field(ge=1, le=2, description="Vendor ID (1=CMT, 2=VeriFone)")
    tpep_pickup_datetime: datetime
    tpep_dropoff_datetime: datetime
    passenger_count: int = Field(ge=0, le=9)
    trip_distance: float = Field(ge=0, description="Trip distance in miles")
    ratecodeid: int = Field(ge=1, le=6)
    pulocationid: int = Field(ge=1, le=265)
    dolocationid: int = Field(ge=1, le=265)
    payment_type: int = Field(ge=1, le=6)
    fare_amount: float = Field(ge=0)
    extra: float = Field(ge=0)
    mta_tax: float = Field(ge=0)
    tip_amount: float = Field(ge=0)
    tolls_amount: float = Field(ge=0)
    total_amount: float
    
    @field_validator('tpep_dropoff_datetime')
    @classmethod
    def dropoff_after_pickup(cls, v, info):
        """Validate dropoff is after pickup."""
        if 'tpep_pickup_datetime' in info.data:
            if v <= info.data['tpep_pickup_datetime']:
                raise ValueError("Dropoff must be after pickup")
        return v
    
    @field_validator('trip_distance')
    @classmethod
    def reasonable_distance(cls, v):
        """Validate trip distance is reasonable (< 500 miles)."""
        if v > 500:
            raise ValueError("Trip distance exceeds reasonable limit")
        return v


