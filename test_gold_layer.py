"""
Test Gold Layer - Verify all tables and show sample data
"""
import pandas as pd
import os

GOLD_PATH = "./iesco_gold_data"

print("="*80)
print("GOLD LAYER VERIFICATION")
print("="*80)

# List all files
print("\nFiles in Gold Layer:")
for file in sorted(os.listdir(GOLD_PATH)):
    if file.endswith('.parquet'):
        size = os.path.getsize(f"{GOLD_PATH}/{file}") / 1024
        print(f"   {file}: {size:.2f} KB")

print("\n" + "="*80)
print("DIMENSION TABLES")
print("="*80)

# DIM_METER
print("\n1. DIM_METER (first 3 rows):")
dim_meter = pd.read_parquet(f"{GOLD_PATH}/dim_meter.parquet")
print(f"   Total: {len(dim_meter)} records")
print(dim_meter[['meter_key', 'consumer_type', 'district', 'sanctioned_load_kw']].head(3))

# DIM_DATE
print("\n2. DIM_DATE (first 3 rows):")
dim_date = pd.read_parquet(f"{GOLD_PATH}/dim_date.parquet")
print(f"   Total: {len(dim_date)} records")
print(dim_date[['date_key', 'date', 'month_name', 'season']].head(3))

# DIM_TIME
print("\n3. DIM_TIME (sample rows):")
dim_time = pd.read_parquet(f"{GOLD_PATH}/dim_time.parquet")
print(f"   Total: {len(dim_time)} records")
print(dim_time[['time_key', 'hour', 'time_of_day', 'is_peak_hour']].head(5))

# DIM_CONSUMER_TYPE
print("\n4. DIM_CONSUMER_TYPE:")
dim_consumer = pd.read_parquet(f"{GOLD_PATH}/dim_consumer_type.parquet")
print(f"   Total: {len(dim_consumer)} records")
print(dim_consumer)

# DIM_LOCATION
print("\n5. DIM_LOCATION (first 5 rows):")
dim_location = pd.read_parquet(f"{GOLD_PATH}/dim_location.parquet")
print(f"   Total: {len(dim_location)} records")
print(dim_location.head(5))

print("\n" + "="*80)
print("FACT TABLES")
print("="*80)

# FACT_READINGS
print("\n1. FACT_READINGS (first 3 rows):")
fact_readings = pd.read_parquet(f"{GOLD_PATH}/fact_readings.parquet")
print(f"   Total: {len(fact_readings):,} records")
print(fact_readings[['meter_key', 'date_key', 'time_key', 'energy_consumed_kwh', 'voltage_v']].head(3))

# FACT_BILLS
print("\n2. FACT_BILLS (first 3 rows):")
fact_bills = pd.read_parquet(f"{GOLD_PATH}/fact_bills.parquet")
print(f"   Total: {len(fact_bills)} records")
print(fact_bills[['bill_key', 'meter_key', 'consumption_kwh', 'bill_amount', 'status']].head(3))

# FACT_PAYMENTS
print("\n3. FACT_PAYMENTS (first 3 rows):")
fact_payments = pd.read_parquet(f"{GOLD_PATH}/fact_payments.parquet")
print(f"   Total: {len(fact_payments)} records")
print(fact_payments[['payment_key', 'bill_key', 'bill_amount', 'amount_paid', 'status']].head(3))

print("\n" + "="*80)
print("AGGREGATE TABLES")
print("="*80)

# AGG_MONTHLY_CONSUMPTION
print("\n1. AGG_MONTHLY_CONSUMPTION (first 5 rows):")
agg_monthly = pd.read_parquet(f"{GOLD_PATH}/agg_monthly_consumption.parquet")
print(f"   Total: {len(agg_monthly)} records")
print(agg_monthly.head(5))

# AGG_DAILY_CONSUMPTION
print("\n2. AGG_DAILY_CONSUMPTION (first 5 rows):")
agg_daily = pd.read_parquet(f"{GOLD_PATH}/agg_daily_consumption.parquet")
print(f"   Total: {len(agg_daily):,} records")
print(agg_daily.head(5))

# AGG_CONSUMER_TYPE_SUMMARY
print("\n3. AGG_CONSUMER_TYPE_SUMMARY:")
agg_consumer = pd.read_parquet(f"{GOLD_PATH}/agg_consumer_type_summary.parquet")
print(f"   Total: {len(agg_consumer)} records")
if len(agg_consumer) > 0:
    print(agg_consumer)
else:
    print("   [WARNING] No records found!")

# AGG_PAYMENT_SUMMARY
print("\n4. AGG_PAYMENT_SUMMARY (first 5 rows):")
agg_payment = pd.read_parquet(f"{GOLD_PATH}/agg_payment_summary.parquet")
print(f"   Total: {len(agg_payment)} records")
print(agg_payment.head(5))

# AGG_LOCATION_SUMMARY
print("\n5. AGG_LOCATION_SUMMARY:")
agg_location = pd.read_parquet(f"{GOLD_PATH}/agg_location_summary.parquet")
print(f"   Total: {len(agg_location)} records")
if len(agg_location) > 0:
    print(agg_location.head(10))
else:
    print("   [WARNING] No records found!")

print("\n" + "="*80)
print("SUMMARY")
print("="*80)
print(f"\nTotal Dimensions: 5 tables")
print(f"Total Facts: 3 tables")
print(f"Total Aggregates: 5 tables")
print(f"\nGold Layer is ready for analytics!")
print("="*80)
