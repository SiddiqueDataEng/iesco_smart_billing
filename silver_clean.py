"""
IESCO Silver Layer Processing - Clean Version
No special characters, handles large datasets
"""

import pandas as pd
import numpy as np
import os
import json
from datetime import datetime

print("="*80)
print("IESCO SILVER LAYER PROCESSING")
print("="*80)
print("Processing Bronze Layer to Silver Layer (Parquet format)")
print()

# Paths
BRONZE_PATH = "./iesco_complete_data"
SILVER_PATH = "./iesco_silver_data"
os.makedirs(SILVER_PATH, exist_ok=True)

print("="*80)
print("STEP 1: LOAD BRONZE LAYER")
print("="*80)

try:
    print("\nLoading data...")
    meters_bronze = pd.read_csv(f"{BRONZE_PATH}/meters.csv")
    print(f"   [OK] Meters: {len(meters_bronze):,} records")
    
    print("   Loading readings (large file, this may take a minute)...")
    readings_bronze = pd.read_csv(f"{BRONZE_PATH}/readings.csv", low_memory=False)
    print(f"   [OK] Readings: {len(readings_bronze):,} records")
    
    bills_bronze = pd.read_csv(f"{BRONZE_PATH}/bills.csv")
    print(f"   [OK] Bills: {len(bills_bronze):,} records")
    
    payments_bronze = pd.read_csv(f"{BRONZE_PATH}/payments.csv")
    print(f"   [OK] Payments: {len(payments_bronze):,} records")
    
except Exception as e:
    print(f"   [ERROR] Failed to load data: {e}")
    exit(1)

print("\n" + "="*80)
print("STEP 2: CLEAN METERS")
print("="*80)

print("\nCleaning meters...")
meters_clean = meters_bronze.copy()

# Remove duplicates
meters_clean = meters_clean.drop_duplicates(subset=['meter_number'])

# Handle missing values
meters_clean['consumer_type'] = meters_clean['consumer_type'].fillna('UNKNOWN')

# Validate sanctioned_load_kw (convert to numeric first)
meters_clean['sanctioned_load_kw'] = pd.to_numeric(meters_clean['sanctioned_load_kw'], errors='coerce').fillna(5.0)
meters_clean.loc[meters_clean['sanctioned_load_kw'] <= 0, 'sanctioned_load_kw'] = 5.0
meters_clean.loc[meters_clean['sanctioned_load_kw'] > 5000, 'sanctioned_load_kw'] = 5.0

# Standardize text fields
meters_clean['consumer_type'] = meters_clean['consumer_type'].str.upper().str.strip()
if 'district' in meters_clean.columns:
    meters_clean['district'] = meters_clean['district'].str.upper().str.strip()

# Add metadata
meters_clean['processed_at'] = datetime.now()
meters_clean['data_layer'] = 'silver'

print(f"   [OK] Cleaned: {len(meters_clean):,} meters")
print(f"   [OK] Removed: {len(meters_bronze) - len(meters_clean):,} duplicates")

print("\n" + "="*80)
print("STEP 3: CLEAN READINGS")
print("="*80)

print("\nCleaning readings (this will take a few minutes)...")
readings_clean = readings_bronze.copy()

# Remove duplicates
print("   Removing duplicates...")
readings_clean = readings_clean.drop_duplicates(subset=['meter_number', 'timestamp'])

# Convert timestamp
print("   Converting timestamps...")
readings_clean['timestamp'] = pd.to_datetime(readings_clean['timestamp'], errors='coerce')

# Validate energy_consumed_kwh (convert to numeric first)
print("   Validating consumption...")
if 'energy_consumed_kwh' in readings_clean.columns:
    readings_clean['energy_consumed_kwh'] = pd.to_numeric(readings_clean['energy_consumed_kwh'], errors='coerce').fillna(0.0)
    readings_clean.loc[readings_clean['energy_consumed_kwh'] < 0, 'energy_consumed_kwh'] = 0.0
    readings_clean.loc[readings_clean['energy_consumed_kwh'] > 100, 'energy_consumed_kwh'] = 0.0

# Validate voltage (convert to numeric first)
print("   Validating voltage...")
if 'voltage_v' in readings_clean.columns:
    readings_clean['voltage_v'] = pd.to_numeric(readings_clean['voltage_v'], errors='coerce').fillna(230.0)
    readings_clean.loc[readings_clean['voltage_v'] < 150, 'voltage_v'] = 230.0
    readings_clean.loc[readings_clean['voltage_v'] > 450, 'voltage_v'] = 230.0

# Flag anomalies
print("   Flagging anomalies...")
readings_clean = readings_clean.sort_values(['meter_number', 'timestamp'])
readings_clean['reading_kwh'] = pd.to_numeric(readings_clean['reading_kwh'], errors='coerce')
readings_clean['prev_reading'] = readings_clean.groupby('meter_number')['reading_kwh'].shift(1)
readings_clean['is_anomaly'] = (
    (readings_clean['reading_kwh'].isna()) |
    (readings_clean['reading_kwh'] < 0) |
    ((readings_clean['prev_reading'].notna()) & (readings_clean['reading_kwh'] < readings_clean['prev_reading']))
)
readings_clean = readings_clean.drop('prev_reading', axis=1)

# Check referential integrity for readings
print("   Checking referential integrity...")
valid_meters = set(meters_clean['meter_number'])
readings_clean['has_valid_meter'] = readings_clean['meter_number'].isin(valid_meters)
invalid_readings = (~readings_clean['has_valid_meter']).sum()
if invalid_readings > 0:
    print(f"   ⚠️  Found {invalid_readings:,} readings with invalid meter_number - removing...")
    readings_clean = readings_clean[readings_clean['has_valid_meter']].copy()
readings_clean = readings_clean.drop(columns=['has_valid_meter'])

# Add metadata
readings_clean['processed_at'] = datetime.now()
readings_clean['data_layer'] = 'silver'

print(f"   [OK] Cleaned: {len(readings_clean):,} readings")
print(f"   [OK] Removed: {len(readings_bronze) - len(readings_clean):,} duplicates")
anomaly_count = readings_clean['is_anomaly'].sum()
print(f"   [INFO] Anomalies flagged: {anomaly_count:,} ({anomaly_count/len(readings_clean)*100:.2f}%)")

print("\n" + "="*80)
print("STEP 4: CLEAN BILLS")
print("="*80)

print("\nCleaning bills...")
bills_clean = bills_bronze.copy()

# Remove duplicates
bills_clean = bills_clean.drop_duplicates(subset=['bill_id'])

# Validate consumption_kwh (convert to numeric first)
bills_clean['consumption_kwh'] = pd.to_numeric(bills_clean['consumption_kwh'], errors='coerce').fillna(0.0)
bills_clean.loc[bills_clean['consumption_kwh'] < 0, 'consumption_kwh'] = 0.0
bills_clean.loc[bills_clean['consumption_kwh'] > 50000, 'consumption_kwh'] = 0.0

# Validate bill_amount (convert to numeric first)
bills_clean['bill_amount'] = pd.to_numeric(bills_clean['bill_amount'], errors='coerce').fillna(0.0)
bills_clean.loc[bills_clean['bill_amount'] < 0, 'bill_amount'] = 0.0

# Convert dates
bills_clean['issue_date'] = pd.to_datetime(bills_clean['issue_date'], errors='coerce')
bills_clean['due_date'] = pd.to_datetime(bills_clean['due_date'], errors='coerce')

# Check referential integrity
# Note: bills use 'meter_id', meters use 'meter_number' - need to handle both
if 'meter_id' in bills_clean.columns:
    valid_meters = set(meters_clean['meter_number'])
    bills_clean['has_valid_meter'] = bills_clean['meter_id'].isin(valid_meters)
elif 'meter_number' in bills_clean.columns:
    valid_meters = set(meters_clean['meter_number'])
    bills_clean['has_valid_meter'] = bills_clean['meter_number'].isin(valid_meters)

# Add metadata
bills_clean['processed_at'] = datetime.now()
bills_clean['data_layer'] = 'silver'

print(f"   [OK] Cleaned: {len(bills_clean):,} bills")
print(f"   [OK] Removed: {len(bills_bronze) - len(bills_clean):,} duplicates")
invalid_meter_count = (~bills_clean['has_valid_meter']).sum()
if invalid_meter_count > 0:
    print(f"   [WARN] Bills with invalid meters: {invalid_meter_count:,}")

print("\n" + "="*80)
print("STEP 5: CLEAN PAYMENTS")
print("="*80)

print("\nCleaning payments...")
payments_clean = payments_bronze.copy()

# Remove duplicates
payments_clean = payments_clean.drop_duplicates(subset=['payment_id'])

# Validate amounts (convert to numeric first)
payments_clean['bill_amount'] = pd.to_numeric(payments_clean['bill_amount'], errors='coerce').fillna(0.0)
payments_clean.loc[payments_clean['bill_amount'] < 0, 'bill_amount'] = 0.0

payments_clean['amount_paid'] = pd.to_numeric(payments_clean['amount_paid'], errors='coerce').fillna(0.0)
payments_clean.loc[payments_clean['amount_paid'] < 0, 'amount_paid'] = 0.0

# Convert payment_date
payments_clean['payment_date'] = pd.to_datetime(payments_clean['payment_date'], errors='coerce')

# Validate status
valid_statuses = ['paid', 'partial', 'unpaid']
payments_clean['status'] = payments_clean['status'].str.lower()
payments_clean.loc[~payments_clean['status'].isin(valid_statuses), 'status'] = 'unpaid'

# Check referential integrity
# Note: payments use 'bill_id', bills use 'bill_id'
valid_bills = set(bills_clean['bill_id'])
payments_clean['has_valid_bill'] = payments_clean['bill_id'].isin(valid_bills)

# Add metadata
payments_clean['processed_at'] = datetime.now()
payments_clean['data_layer'] = 'silver'

print(f"   [OK] Cleaned: {len(payments_clean):,} payments")
print(f"   [OK] Removed: {len(payments_bronze) - len(payments_clean):,} duplicates")
invalid_bill_count = (~payments_clean['has_valid_bill']).sum()
if invalid_bill_count > 0:
    print(f"   [WARN] Payments with invalid bills: {invalid_bill_count:,}")

print("\n" + "="*80)
print("STEP 6: SAVE SILVER LAYER (PARQUET FORMAT)")
print("="*80)

print("\nSaving cleaned data to Parquet format...")
print("(Parquet is compressed and 10x faster than CSV)")

# Save as Parquet (compressed)
print("   Saving meters...")
meters_clean.to_parquet(f"{SILVER_PATH}/meters.parquet", compression='snappy', index=False)
meters_size = os.path.getsize(f"{SILVER_PATH}/meters.parquet") / (1024 * 1024)
print(f"   [OK] Meters saved: {SILVER_PATH}/meters.parquet ({meters_size:.2f} MB)")

print("   Saving readings (large file, please wait)...")
readings_clean.to_parquet(f"{SILVER_PATH}/readings.parquet", compression='snappy', index=False)
readings_size = os.path.getsize(f"{SILVER_PATH}/readings.parquet") / (1024 * 1024)
print(f"   [OK] Readings saved: {SILVER_PATH}/readings.parquet ({readings_size:.2f} MB)")

print("   Saving bills...")
bills_clean.to_parquet(f"{SILVER_PATH}/bills.parquet", compression='snappy', index=False)
bills_size = os.path.getsize(f"{SILVER_PATH}/bills.parquet") / (1024 * 1024)
print(f"   [OK] Bills saved: {SILVER_PATH}/bills.parquet ({bills_size:.2f} MB)")

print("   Saving payments...")
payments_clean.to_parquet(f"{SILVER_PATH}/payments.parquet", compression='snappy', index=False)
payments_size = os.path.getsize(f"{SILVER_PATH}/payments.parquet") / (1024 * 1024)
print(f"   [OK] Payments saved: {SILVER_PATH}/payments.parquet ({payments_size:.2f} MB)")

total_size = meters_size + readings_size + bills_size + payments_size
print(f"\n   [OK] Total Silver Layer size: {total_size:.2f} MB")

print("\n" + "="*80)
print("STEP 7: GENERATE QUALITY REPORT")
print("="*80)

# Calculate statistics
paid_count = (payments_clean['status'] == 'paid').sum()
partial_count = (payments_clean['status'] == 'partial').sum()
unpaid_count = (payments_clean['status'] == 'unpaid').sum()

quality_report = {
    'processing_date': datetime.now().isoformat(),
    'bronze_layer': BRONZE_PATH,
    'silver_layer': SILVER_PATH,
    'total_meters': int(len(meters_clean)),
    'total_readings': int(len(readings_clean)),
    'total_bills': int(len(bills_clean)),
    'total_payments': int(len(payments_clean)),
    'anomaly_count': int(anomaly_count),
    'anomaly_percentage': float(anomaly_count/len(readings_clean)*100),
    'paid_count': int(paid_count),
    'partial_count': int(partial_count),
    'unpaid_count': int(unpaid_count),
    'paid_percentage': float(paid_count/len(payments_clean)*100),
    'unpaid_percentage': float(unpaid_count/len(payments_clean)*100),
    'file_sizes_mb': {
        'meters': float(meters_size),
        'readings': float(readings_size),
        'bills': float(bills_size),
        'payments': float(payments_size),
        'total': float(total_size)
    }
}

with open(f"{SILVER_PATH}/quality_report.json", 'w') as f:
    json.dump(quality_report, f, indent=2)

print(f"\nSilver Layer Statistics:")
print(f"   Meters: {quality_report['total_meters']:,}")
print(f"   Readings: {quality_report['total_readings']:,}")
print(f"   Bills: {quality_report['total_bills']:,}")
print(f"   Payments: {quality_report['total_payments']:,}")

print(f"\nData Quality:")
print(f"   Reading anomalies: {quality_report['anomaly_percentage']:.2f}%")

print(f"\nPayment Statistics:")
print(f"   Paid: {paid_count:,} ({quality_report['paid_percentage']:.1f}%)")
print(f"   Partial: {partial_count:,} ({partial_count/len(payments_clean)*100:.1f}%)")
print(f"   Unpaid: {unpaid_count:,} ({quality_report['unpaid_percentage']:.1f}%)")

print(f"\n   [OK] Quality report saved: {SILVER_PATH}/quality_report.json")

print("\n" + "="*80)
print("SUCCESS - SILVER LAYER PROCESSING COMPLETE")
print("="*80)
print(f"\nBronze Layer (CSV): {BRONZE_PATH}/")
print(f"Silver Layer (Parquet): {SILVER_PATH}/")
print(f"\nTotal size: {total_size:.2f} MB (compressed)")
print(f"Data is now cleaned, validated, and ready for analytics!")
print("\nNext step: Load Parquet files for analysis:")
print(f"  import pandas as pd")
print(f"  df = pd.read_parquet('{SILVER_PATH}/readings.parquet')")
print("="*80)
