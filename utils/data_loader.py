"""
Data Loading Utilities
"""
import pandas as pd
import streamlit as st
from pathlib import Path

@st.cache_data
def load_all_gold_data():
    """Load all Gold Layer tables"""
    gold_path = Path("./iesco_gold_data")
    
    if not gold_path.exists():
        st.error(f"Gold Layer data not found at {gold_path}")
        return None
    
    data = {}
    try:
        # Dimensions
        data['dim_meter'] = pd.read_parquet(gold_path / "dim_meter.parquet")
        data['dim_date'] = pd.read_parquet(gold_path / "dim_date.parquet")
        data['dim_time'] = pd.read_parquet(gold_path / "dim_time.parquet")
        data['dim_consumer_type'] = pd.read_parquet(gold_path / "dim_consumer_type.parquet")
        data['dim_location'] = pd.read_parquet(gold_path / "dim_location.parquet")
        
        # Facts
        data['fact_readings'] = pd.read_parquet(gold_path / "fact_readings.parquet")
        data['fact_bills'] = pd.read_parquet(gold_path / "fact_bills.parquet")
        data['fact_payments'] = pd.read_parquet(gold_path / "fact_payments.parquet")
        
        # Aggregates
        data['agg_monthly'] = pd.read_parquet(gold_path / "agg_monthly_consumption.parquet")
        data['agg_daily'] = pd.read_parquet(gold_path / "agg_daily_consumption.parquet")
        data['agg_consumer_type'] = pd.read_parquet(gold_path / "agg_consumer_type_summary.parquet")
        data['agg_payment'] = pd.read_parquet(gold_path / "agg_payment_summary.parquet")
        data['agg_location'] = pd.read_parquet(gold_path / "agg_location_summary.parquet")
        
        return data
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return None

def get_enriched_readings(data):
    """Get readings enriched with meter and location info"""
    readings = data['fact_readings'].copy()
    meters = data['dim_meter'].copy()
    
    # Convert meter_key to string for joining
    readings['meter_key'] = readings['meter_key'].astype(str)
    meters['meter_key'] = meters['meter_key'].astype(str)
    
    enriched = readings.merge(
        meters[['meter_key', 'consumer_type', 'district', 'division', 'sanctioned_load_kw']],
        on='meter_key',
        how='left'
    )
    
    return enriched

def get_enriched_bills(data):
    """Get bills enriched with meter info"""
    bills = data['fact_bills'].copy()
    meters = data['dim_meter'].copy()
    
    # Convert meter_key to string
    bills['meter_key'] = bills['meter_key'].astype(str)
    meters['meter_key'] = meters['meter_key'].astype(str)
    
    enriched = bills.merge(
        meters[['meter_key', 'consumer_type', 'district', 'tariff_category']],
        on='meter_key',
        how='left'
    )
    
    return enriched
