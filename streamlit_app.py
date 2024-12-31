"""
IESCO Smart Billing Analytics Dashboard
========================================
Multi-page Streamlit dashboard for comprehensive analytics and ML models
"""

import streamlit as st
import pandas as pd
import numpy as np
from pathlib import Path

# Page configuration
st.set_page_config(
    page_title="IESCO Analytics Dashboard",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        padding: 1rem;
        background: linear-gradient(90deg, #e3f2fd 0%, #bbdefb 100%);
        border-radius: 10px;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #1f77b4;
    }
    .stMetric {
        background-color: white;
        padding: 10px;
        border-radius: 5px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
</style>
""", unsafe_allow_html=True)

# Main header
st.markdown('<div class="main-header">⚡ IESCO Smart Billing Analytics Dashboard</div>', unsafe_allow_html=True)

# Sidebar navigation
st.sidebar.title("📊 Navigation")
st.sidebar.markdown("---")

# Page categories
page_category = st.sidebar.radio(
    "Select Category:",
    ["🏠 Home", "📈 Analytics", "🤖 ML Models", "🗺️ Geospatial", "⚙️ Operations"]
)

# Initialize session state for data caching
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False
    st.session_state.gold_path = "./iesco_gold_data"

# Data loading function
@st.cache_data
def load_gold_data():
    """Load all Gold Layer tables"""
    gold_path = Path("./iesco_gold_data")
    
    if not gold_path.exists():
        st.error(f"Gold Layer data not found at {gold_path}")
        return None
    
    data = {}
    try:
        # Load dimension tables
        data['dim_meter'] = pd.read_parquet(gold_path / "dim_meter.parquet")
        data['dim_date'] = pd.read_parquet(gold_path / "dim_date.parquet")
        data['dim_time'] = pd.read_parquet(gold_path / "dim_time.parquet")
        data['dim_consumer_type'] = pd.read_parquet(gold_path / "dim_consumer_type.parquet")
        data['dim_location'] = pd.read_parquet(gold_path / "dim_location.parquet")
        
        # Load fact tables
        data['fact_readings'] = pd.read_parquet(gold_path / "fact_readings.parquet")
        data['fact_bills'] = pd.read_parquet(gold_path / "fact_bills.parquet")
        data['fact_payments'] = pd.read_parquet(gold_path / "fact_payments.parquet")
        
        # Load aggregate tables
        data['agg_monthly'] = pd.read_parquet(gold_path / "agg_monthly_consumption.parquet")
        data['agg_daily'] = pd.read_parquet(gold_path / "agg_daily_consumption.parquet")
        data['agg_consumer_type'] = pd.read_parquet(gold_path / "agg_consumer_type_summary.parquet")
        data['agg_payment'] = pd.read_parquet(gold_path / "agg_payment_summary.parquet")
        data['agg_location'] = pd.read_parquet(gold_path / "agg_location_summary.parquet")
        
        return data
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return None

# Home Page
if page_category == "🏠 Home":
    st.header("Welcome to IESCO Analytics Dashboard")
    
    st.markdown("""
    ### 📊 Comprehensive Analytics & ML Platform
    
    This dashboard provides advanced analytics and machine learning capabilities for IESCO smart billing data.
    
    #### 🎯 Key Features:
    """)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        **📈 Analytics**
        - Billing Accuracy Analysis
        - Grid Stability Monitoring
        - Consumer Segmentation
        - Data Quality Tracking
        - Performance Analytics
        """)
    
    with col2:
        st.markdown("""
        **🤖 ML Models**
        - Load Forecasting
        - Tamper Detection
        - Failure Prediction
        - Churn Analysis
        - Upgrade Prediction
        """)
    
    with col3:
        st.markdown("""
        **🗺️ Geospatial**
        - Regional Analysis
        - Infrastructure Planning
        - Capacity Planning
        - Zone-wise Forecasting
        - Solar Impact Study
        """)
    
    st.markdown("---")
    
    # Load and display summary statistics
    with st.spinner("Loading data..."):
        data = load_gold_data()
    
    if data:
        st.success("✅ Data loaded successfully!")
        
        st.subheader("📊 Data Summary")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Meters", f"{len(data['dim_meter']):,}")
        
        with col2:
            st.metric("Total Readings", f"{len(data['fact_readings']):,}")
        
        with col3:
            st.metric("Total Bills", f"{len(data['fact_bills']):,}")
        
        with col4:
            st.metric("Consumer Types", f"{len(data['dim_consumer_type']):,}")
        
        st.markdown("---")
        
        # Quick insights
        st.subheader("🔍 Quick Insights")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Top 5 Consumer Types by Consumption**")
            top_consumers = data['agg_consumer_type'].nlargest(5, 'total_consumption_kwh')
            st.dataframe(
                top_consumers[['consumer_type', 'total_consumption_kwh', 'meter_count']],
                hide_index=True
            )
        
        with col2:
            st.markdown("**Payment Status Distribution**")
            payment_dist = data['agg_payment'].groupby('payment_status')['payment_count'].sum()
            st.bar_chart(payment_dist)
        
        st.markdown("---")
        st.info("👈 Use the sidebar to navigate to different analytics and ML modules")

# Analytics Pages
elif page_category == "📈 Analytics":
    page = st.sidebar.selectbox(
        "Select Analytics Module:",
        [
            "Billing Accuracy",
            "Grid Stability Analysis",
            "Consumer Segmentation",
            "Data Quality Monitoring",
            "Meter Lifecycle Analysis",
            "Solar Integration Impact",
            "Tariff Change Impact",
            "District Performance"
        ]
    )
    
    st.header(f"📈 {page}")
    st.info(f"Loading {page} module...")
    st.markdown(f"**Module**: `pages/analytics/{page.lower().replace(' ', '_')}.py`")

# ML Models Pages
elif page_category == "🤖 ML Models":
    page = st.sidebar.selectbox(
        "Select ML Model:",
        [
            "Load Forecasting",
            "Tamper Detection",
            "Transformer Upgrade Prediction",
            "Churn Analysis",
            "Failure Pattern Analysis",
            "Replacement Optimization",
            "New Connection Forecasting",
            "Seasonal Capacity Planning"
        ]
    )
    
    st.header(f"🤖 {page}")
    st.info(f"Loading {page} module...")
    st.markdown(f"**Module**: `pages/ml_models/{page.lower().replace(' ', '_')}.py`")

# Geospatial Pages
elif page_category == "🗺️ Geospatial":
    page = st.sidebar.selectbox(
        "Select Geospatial Module:",
        [
            "Regional Consumption Forecasting",
            "Infrastructure Planning",
            "Solar Impact Study",
            "Zone-wise Growth Analysis",
            "Geospatial Heatmaps"
        ]
    )
    
    st.header(f"🗺️ {page}")
    st.info(f"Loading {page} module...")
    st.markdown(f"**Module**: `pages/geospatial/{page.lower().replace(' ', '_')}.py`")

# Operations Pages
elif page_category == "⚙️ Operations":
    page = st.sidebar.selectbox(
        "Select Operations Module:",
        [
            "ETL Pipeline Monitor",
            "Data Warehouse Status",
            "System Health Dashboard"
        ]
    )
    
    st.header(f"⚙️ {page}")
    st.info(f"Loading {page} module...")
    st.markdown(f"**Module**: `pages/operations/{page.lower().replace(' ', '_')}.py`")

# Footer
st.sidebar.markdown("---")
st.sidebar.markdown("""
<div style='text-align: center; color: #666; font-size: 0.8rem;'>
    <p>IESCO Smart Billing Analytics</p>
    <p>Version 2.0</p>
</div>
""", unsafe_allow_html=True)
