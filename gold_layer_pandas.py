"""
IESCO Gold Layer Processing - Pandas Version
=============================================
Creates Star Schema with Dimension and Fact tables
No Spark required - uses Pandas
"""

import pandas as pd
import numpy as np
import os
import json
from datetime import datetime, timedelta

print("="*80)
print("IESCO GOLD LAYER PROCESSING - Star Schema (Pandas)")
print("="*80)
print("Creating Dimensional Model (DIMs + FACTs)")
print()

# Paths
SILVER_PATH = "./iesco_silver_data"
GOLD_PATH = "./iesco_gold_data"
os.makedirs(GOLD_PATH, exist_ok=True)

print("="*80)
print("STEP 1: LOAD SILVER LAYER")
print("="*80)

try:
    print("\nLoading Silver Layer (Parquet)...")
    meters_silver = pd.read_parquet(f"{SILVER_PATH}/meters.parquet")
    print(f"   [OK] Meters: {len(meters_silver):,}")
    
    print("   Loading readings (large file)...")
    readings_silver = pd.read_parquet(f"{SILVER_PATH}/readings.parquet")
    print(f"   [OK] Readings: {len(readings_silver):,}")
    
    bills_silver = pd.read_parquet(f"{SILVER_PATH}/bills.parquet")
    print(f"   [OK] Bills: {len(bills_silver):,}")
    
    payments_silver = pd.read_parquet(f"{SILVER_PATH}/payments.parquet")
    print(f"   [OK] Payments: {len(payments_silver):,}")
    
except Exception as e:
    print(f"   [ERROR] Failed to load Silver Layer: {e}")
    exit(1)

print("\n" + "="*80)
print("STEP 2: CREATE DIMENSION TABLES")
print("="*80)

# DIM_METER
print("\n1. Creating DIM_METER...")
dim_meter = meters_silver[[
    'meter_number', 'consumer_id', 'consumer_type', 'tariff_category',
    'consumer_category', 'phase_type', 'meter_type', 'sanctioned_load_kw',
    'connected_load_kw', 'has_solar', 'solar_capacity_kw',
    'district', 'division', 'sub_division', 'installation_date', 'status'
]].copy()
dim_meter['meter_key'] = dim_meter['meter_number']
dim_meter['dim_created_at'] = datetime.now()
dim_meter.to_parquet(f"{GOLD_PATH}/dim_meter.parquet", index=False)
print(f"   [OK] DIM_METER: {len(dim_meter):,} records")

# DIM_DATE
print("\n2. Creating DIM_DATE...")
readings_silver['timestamp'] = pd.to_datetime(readings_silver['timestamp'])
min_date = readings_silver['timestamp'].min().date()
max_date = readings_silver['timestamp'].max().date()
date_range = pd.date_range(start=min_date, end=max_date, freq='D')

dim_date = pd.DataFrame({
    'date': date_range,
    'date_key': date_range.strftime('%Y%m%d').astype(int),
    'year': date_range.year,
    'month': date_range.month,
    'day': date_range.day,
    'day_of_week': date_range.dayofweek + 1,
    'day_of_year': date_range.dayofyear,
    'week_of_year': date_range.isocalendar().week,
    'quarter': date_range.quarter,
    'month_name': date_range.strftime('%B'),
    'day_name': date_range.strftime('%A'),
    'is_weekend': date_range.dayofweek.isin([5, 6]),
    'season': date_range.month.map({
        12: 'Winter', 1: 'Winter', 2: 'Winter',
        3: 'Spring', 4: 'Spring', 5: 'Spring',
        6: 'Summer', 7: 'Summer', 8: 'Summer',
        9: 'Fall', 10: 'Fall', 11: 'Fall'
    })
})
dim_date.to_parquet(f"{GOLD_PATH}/dim_date.parquet", index=False)
print(f"   [OK] DIM_DATE: {len(dim_date):,} records")

# DIM_TIME
print("\n3. Creating DIM_TIME...")
dim_time = pd.DataFrame({
    'hour': range(24),
    'time_key': [h * 100 for h in range(24)],
    'am_pm': ['AM' if h < 12 else 'PM' for h in range(24)],
    'time_of_day': [
        'Night' if h < 6 else 'Morning' if h < 12 else 
        'Afternoon' if h < 18 else 'Evening' if h < 22 else 'Night'
        for h in range(24)
    ],
    'is_peak_hour': [17 <= h <= 22 for h in range(24)],
    'is_off_peak': [0 <= h <= 5 for h in range(24)]
})
dim_time.to_parquet(f"{GOLD_PATH}/dim_time.parquet", index=False)
print(f"   [OK] DIM_TIME: {len(dim_time):,} records")

# DIM_CONSUMER_TYPE
print("\n4. Creating DIM_CONSUMER_TYPE...")
dim_consumer_type = meters_silver[[
    'consumer_type', 'consumer_category', 'tariff_category'
]].drop_duplicates()
dim_consumer_type['consumer_type_key'] = dim_consumer_type['consumer_type']
dim_consumer_type.to_parquet(f"{GOLD_PATH}/dim_consumer_type.parquet", index=False)
print(f"   [OK] DIM_CONSUMER_TYPE: {len(dim_consumer_type):,} records")

# DIM_LOCATION
print("\n5. Creating DIM_LOCATION...")
dim_location = meters_silver[[
    'district', 'division', 'sub_division'
]].drop_duplicates()
dim_location['location_key'] = (
    dim_location['district'] + '-' + 
    dim_location['division'] + '-' + 
    dim_location['sub_division']
)
dim_location.to_parquet(f"{GOLD_PATH}/dim_location.parquet", index=False)
print(f"   [OK] DIM_LOCATION: {len(dim_location):,} records")

print("\n" + "="*80)
print("STEP 3: CREATE FACT TABLES")
print("="*80)

# FACT_READINGS
print("\n1. Creating FACT_READINGS...")
print("   Filtering clean readings...")
fact_readings = readings_silver[readings_silver['is_anomaly'] == False].copy()

fact_readings['meter_key'] = fact_readings['meter_number']
fact_readings['timestamp'] = pd.to_datetime(fact_readings['timestamp'], errors='coerce')
fact_readings = fact_readings.dropna(subset=['timestamp'])  # Remove rows with invalid timestamps
fact_readings['date_key'] = fact_readings['timestamp'].dt.strftime('%Y%m%d').astype(int)
fact_readings['time_key'] = fact_readings['timestamp'].dt.hour * 100

# Add is_peak_hour if not present
if 'is_peak_hour' not in fact_readings.columns:
    fact_readings['is_peak_hour'] = fact_readings['timestamp'].dt.hour.between(17, 22)

# Rename consumption_kwh to energy_consumed_kwh for consistency
if 'consumption_kwh' in fact_readings.columns and 'energy_consumed_kwh' not in fact_readings.columns:
    fact_readings['energy_consumed_kwh'] = fact_readings['consumption_kwh']

# Add data_quality_flag if not present
if 'data_quality_flag' not in fact_readings.columns:
    fact_readings['data_quality_flag'] = 'clean'

fact_readings_final = fact_readings[[
    'meter_key', 'date_key', 'time_key',
    'reading_kwh', 'energy_consumed_kwh', 'voltage_v', 'current_a',
    'power_factor', 'frequency_hz', 'is_anomaly', 'is_peak_hour',
    'timestamp', 'data_quality_flag'
]].copy()

# Convert Arrow types to regular pandas types to avoid aggregation issues
print("   Converting data types...")
for col in fact_readings_final.columns:
    if hasattr(fact_readings_final[col].dtype, 'pyarrow_dtype'):
        fact_readings_final[col] = fact_readings_final[col].astype(str) if fact_readings_final[col].dtype == 'string' else fact_readings_final[col].to_numpy()

print("   Saving (this may take a minute)...")
fact_readings_final.to_parquet(f"{GOLD_PATH}/fact_readings.parquet", index=False)
print(f"   [OK] FACT_READINGS: {len(fact_readings_final):,} records")

# FACT_BILLS
print("\n2. Creating FACT_BILLS...")
fact_bills = bills_silver.copy()
fact_bills['bill_key'] = fact_bills['bill_id']
fact_bills['meter_key'] = fact_bills['meter_id']
fact_bills['billing_month_key'] = pd.to_datetime(fact_bills['billing_month']).dt.strftime('%Y%m').astype(int)
fact_bills['issue_date_key'] = pd.to_datetime(fact_bills['issue_date']).dt.strftime('%Y%m%d').astype(int)
fact_bills['due_date_key'] = pd.to_datetime(fact_bills['due_date']).dt.strftime('%Y%m%d').astype(int)
fact_bills['reading_difference'] = fact_bills['current_reading'] - fact_bills['previous_reading']
fact_bills['rate_per_kwh'] = fact_bills['bill_amount'] / fact_bills['consumption_kwh'].replace(0, np.nan)

fact_bills_final = fact_bills[[
    'bill_key', 'meter_key', 'billing_month_key', 'issue_date_key', 'due_date_key',
    'consumption_kwh', 'bill_amount', 'previous_reading', 'current_reading',
    'reading_difference', 'rate_per_kwh', 'billing_month', 'issue_date',
    'due_date', 'reading_date', 'status'
]]

fact_bills_final.to_parquet(f"{GOLD_PATH}/fact_bills.parquet", index=False)
print(f"   [OK] FACT_BILLS: {len(fact_bills_final):,} records")

# FACT_PAYMENTS
print("\n3. Creating FACT_PAYMENTS...")
fact_payments = payments_silver.merge(
    bills_silver[['bill_id', 'meter_id', 'billing_month']],
    on='bill_id',
    how='left'
)

fact_payments['payment_key'] = fact_payments['payment_id']
fact_payments['bill_key'] = fact_payments['bill_id']
# Use meter_id from the merge, or meter_id from payments if available
if 'meter_id_y' in fact_payments.columns:
    fact_payments['meter_key'] = fact_payments['meter_id_y']
elif 'meter_id_x' in fact_payments.columns:
    fact_payments['meter_key'] = fact_payments['meter_id_x']
elif 'meter_id' in fact_payments.columns:
    fact_payments['meter_key'] = fact_payments['meter_id']
else:
    # Fallback: get from bill_id
    fact_payments['meter_key'] = fact_payments['bill_key'].str.extract(r'MTR-(\d+)')[0]

fact_payments['billing_month_key'] = pd.to_datetime(fact_payments['billing_month'], errors='coerce').dt.strftime('%Y%m').astype('Int64')
fact_payments['payment_date_key'] = pd.to_datetime(fact_payments['payment_date'], errors='coerce').dt.strftime('%Y%m%d').astype('Int64')
fact_payments['amount_due'] = fact_payments['bill_amount'] - fact_payments['amount_paid']
fact_payments['payment_percentage'] = (fact_payments['amount_paid'] / fact_payments['bill_amount'] * 100).fillna(0)

fact_payments_final = fact_payments[[
    'payment_key', 'bill_key', 'meter_key', 'billing_month_key', 'payment_date_key',
    'bill_amount', 'amount_paid', 'amount_due', 'payment_percentage',
    'payment_date', 'payment_method', 'status'
]]

fact_payments_final.to_parquet(f"{GOLD_PATH}/fact_payments.parquet", index=False)
print(f"   [OK] FACT_PAYMENTS: {len(fact_payments_final):,} records")

print("\n" + "="*80)
print("STEP 4: CREATE AGGREGATE TABLES")
print("="*80)

# AGG_MONTHLY_CONSUMPTION
print("\n1. Creating AGG_MONTHLY_CONSUMPTION...")
fact_readings_final['year_month'] = (fact_readings_final['date_key'] / 100).astype(int)

# Ensure numeric columns are proper float types
numeric_cols = ['energy_consumed_kwh', 'voltage_v', 'power_factor']
for col in numeric_cols:
    fact_readings_final[col] = pd.to_numeric(fact_readings_final[col], errors='coerce')

agg_monthly = fact_readings_final.groupby(['meter_key', 'year_month']).agg({
    'energy_consumed_kwh': ['sum', 'mean', 'max', 'min'],
    'voltage_v': 'mean',
    'power_factor': 'mean'
})
agg_monthly['reading_count'] = fact_readings_final.groupby(['meter_key', 'year_month']).size()
agg_monthly.columns = ['total_consumption_kwh', 'avg_consumption_kwh', 'max_consumption_kwh', 
                       'min_consumption_kwh', 'avg_voltage', 'avg_power_factor', 'reading_count']
agg_monthly = agg_monthly.reset_index()
agg_monthly.to_parquet(f"{GOLD_PATH}/agg_monthly_consumption.parquet", index=False)
print(f"   [OK] AGG_MONTHLY_CONSUMPTION: {len(agg_monthly):,} records")

# AGG_DAILY_CONSUMPTION
print("\n2. Creating AGG_DAILY_CONSUMPTION...")
# Ensure numeric column is proper float type
fact_readings_final['energy_consumed_kwh'] = pd.to_numeric(fact_readings_final['energy_consumed_kwh'], errors='coerce')

agg_daily = fact_readings_final.groupby(['meter_key', 'date_key']).agg({
    'energy_consumed_kwh': ['sum', 'mean', 'max']
})
agg_daily['reading_count'] = fact_readings_final.groupby(['meter_key', 'date_key']).size()
agg_daily.columns = ['total_consumption_kwh', 'avg_consumption_kwh', 'max_consumption_kwh', 'reading_count']
agg_daily = agg_daily.reset_index()
agg_daily.to_parquet(f"{GOLD_PATH}/agg_daily_consumption.parquet", index=False)
print(f"   [OK] AGG_DAILY_CONSUMPTION: {len(agg_daily):,} records")

# AGG_CONSUMER_TYPE_SUMMARY
print("\n3. Creating AGG_CONSUMER_TYPE_SUMMARY...")
# Ensure meter_key types match for merge
dim_meter_copy = dim_meter.copy()
dim_meter_copy['meter_key'] = dim_meter_copy['meter_key'].astype(str)
fact_readings_final['meter_key'] = fact_readings_final['meter_key'].astype(str)

readings_with_type = fact_readings_final.merge(
    dim_meter_copy[['meter_key', 'consumer_type', 'consumer_category']],
    on='meter_key'
)
# Ensure numeric columns are proper float types
for col in ['energy_consumed_kwh', 'voltage_v', 'power_factor']:
    readings_with_type[col] = pd.to_numeric(readings_with_type[col], errors='coerce')

agg_consumer = readings_with_type.groupby(['consumer_type', 'consumer_category']).agg({
    'meter_key': 'nunique',
    'energy_consumed_kwh': ['sum', 'mean'],
    'voltage_v': 'mean',
    'power_factor': 'mean'
}).reset_index()
agg_consumer.columns = ['consumer_type', 'consumer_category', 'meter_count',
                        'total_consumption_kwh', 'avg_consumption_kwh',
                        'avg_voltage', 'avg_power_factor']
agg_consumer.to_parquet(f"{GOLD_PATH}/agg_consumer_type_summary.parquet", index=False)
print(f"   [OK] AGG_CONSUMER_TYPE_SUMMARY: {len(agg_consumer):,} records")

# AGG_PAYMENT_SUMMARY
print("\n4. Creating AGG_PAYMENT_SUMMARY...")
agg_payment = fact_payments_final.groupby(['billing_month_key', 'status']).agg({
    'payment_key': 'count',
    'bill_amount': 'sum',
    'amount_paid': 'sum',
    'amount_due': 'sum',
    'payment_percentage': 'mean'
}).reset_index()
agg_payment.columns = ['billing_month_key', 'payment_status', 'payment_count',
                       'total_billed', 'total_paid', 'total_due', 'avg_payment_percentage']
agg_payment.to_parquet(f"{GOLD_PATH}/agg_payment_summary.parquet", index=False)
print(f"   [OK] AGG_PAYMENT_SUMMARY: {len(agg_payment):,} records")

# AGG_LOCATION_SUMMARY
print("\n5. Creating AGG_LOCATION_SUMMARY...")
# Ensure meter_key types match for merge
dim_meter_copy2 = dim_meter.copy()
dim_meter_copy2['meter_key'] = dim_meter_copy2['meter_key'].astype(str)
fact_readings_final['meter_key'] = fact_readings_final['meter_key'].astype(str)

readings_with_location = fact_readings_final.merge(
    dim_meter_copy2[['meter_key', 'district', 'division', 'sub_division']],
    on='meter_key'
)
# Ensure numeric columns are proper float types
for col in ['energy_consumed_kwh', 'voltage_v']:
    readings_with_location[col] = pd.to_numeric(readings_with_location[col], errors='coerce')

agg_location = readings_with_location.groupby(['district', 'division', 'sub_division']).agg({
    'meter_key': 'nunique',
    'energy_consumed_kwh': ['sum', 'mean'],
    'voltage_v': 'mean'
}).reset_index()
agg_location.columns = ['district', 'division', 'sub_division', 'meter_count',
                        'total_consumption_kwh', 'avg_consumption_kwh', 'avg_voltage']
agg_location.to_parquet(f"{GOLD_PATH}/agg_location_summary.parquet", index=False)
print(f"   [OK] AGG_LOCATION_SUMMARY: {len(agg_location):,} records")

print("\n" + "="*80)
print("STEP 5: CREATE METADATA")
print("="*80)

# Calculate sizes
def get_file_size(path):
    return os.path.getsize(path) / (1024 * 1024)

metadata = {
    'created_at': datetime.now().isoformat(),
    'source': SILVER_PATH,
    'destination': GOLD_PATH,
    'schema_type': 'Star Schema',
    'dimensions': {
        'dim_meter': {'records': len(dim_meter), 'size_mb': get_file_size(f"{GOLD_PATH}/dim_meter.parquet")},
        'dim_date': {'records': len(dim_date), 'size_mb': get_file_size(f"{GOLD_PATH}/dim_date.parquet")},
        'dim_time': {'records': len(dim_time), 'size_mb': get_file_size(f"{GOLD_PATH}/dim_time.parquet")},
        'dim_consumer_type': {'records': len(dim_consumer_type), 'size_mb': get_file_size(f"{GOLD_PATH}/dim_consumer_type.parquet")},
        'dim_location': {'records': len(dim_location), 'size_mb': get_file_size(f"{GOLD_PATH}/dim_location.parquet")}
    },
    'facts': {
        'fact_readings': {'records': len(fact_readings_final), 'size_mb': get_file_size(f"{GOLD_PATH}/fact_readings.parquet")},
        'fact_bills': {'records': len(fact_bills_final), 'size_mb': get_file_size(f"{GOLD_PATH}/fact_bills.parquet")},
        'fact_payments': {'records': len(fact_payments_final), 'size_mb': get_file_size(f"{GOLD_PATH}/fact_payments.parquet")}
    },
    'aggregates': {
        'agg_monthly_consumption': {'records': len(agg_monthly), 'size_mb': get_file_size(f"{GOLD_PATH}/agg_monthly_consumption.parquet")},
        'agg_daily_consumption': {'records': len(agg_daily), 'size_mb': get_file_size(f"{GOLD_PATH}/agg_daily_consumption.parquet")},
        'agg_consumer_type_summary': {'records': len(agg_consumer), 'size_mb': get_file_size(f"{GOLD_PATH}/agg_consumer_type_summary.parquet")},
        'agg_payment_summary': {'records': len(agg_payment), 'size_mb': get_file_size(f"{GOLD_PATH}/agg_payment_summary.parquet")},
        'agg_location_summary': {'records': len(agg_location), 'size_mb': get_file_size(f"{GOLD_PATH}/agg_location_summary.parquet")}
    }
}

with open(f"{GOLD_PATH}/metadata.json", 'w') as f:
    json.dump(metadata, f, indent=2)

print("\n[OK] Metadata saved")

print("\n" + "="*80)
print("STEP 6: SUMMARY")
print("="*80)

print("\nGold Layer Summary:")
print(f"\nDimension Tables:")
for name, info in metadata['dimensions'].items():
    print(f"   {name}: {info['records']:,} records ({info['size_mb']:.2f} MB)")

print(f"\nFact Tables:")
for name, info in metadata['facts'].items():
    print(f"   {name}: {info['records']:,} records ({info['size_mb']:.2f} MB)")

print(f"\nAggregate Tables:")
for name, info in metadata['aggregates'].items():
    print(f"   {name}: {info['records']:,} records ({info['size_mb']:.2f} MB)")

total_size = sum(d['size_mb'] for d in metadata['dimensions'].values()) + \
             sum(f['size_mb'] for f in metadata['facts'].values()) + \
             sum(a['size_mb'] for a in metadata['aggregates'].values())

print(f"\nTotal Gold Layer Size: {total_size:.2f} MB")

print("\n" + "="*80)
print("SUCCESS - GOLD LAYER COMPLETE")
print("="*80)
print(f"\nSilver Layer: {SILVER_PATH}/")
print(f"Gold Layer: {GOLD_PATH}/")
print(f"\nStar Schema created with:")
print(f"   5 Dimension tables (DIMs)")
print(f"   3 Fact tables (FACTs)")
print(f"   5 Aggregate tables (AGGs)")
print(f"\nData is now ready for analytics, BI tools, and ML!")
print("="*80)
