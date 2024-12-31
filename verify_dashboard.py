"""
Verify Dashboard Setup
"""
import os
from pathlib import Path

print("="*80)
print("IESCO ANALYTICS DASHBOARD - VERIFICATION")
print("="*80)
print()

# Check main files
print("📄 Checking Main Files...")
main_files = [
    "streamlit_app.py",
    "run_dashboard.bat",
    "requirements_dashboard.txt",
    "DASHBOARD_README.md",
    "DASHBOARD_SUMMARY.md"
]

for file in main_files:
    if os.path.exists(file):
        print(f"   ✅ {file}")
    else:
        print(f"   ❌ {file} - MISSING!")

print()

# Check utils
print("🔧 Checking Utility Modules...")
utils_files = [
    "utils/data_loader.py",
    "utils/ml_models.py"
]

for file in utils_files:
    if os.path.exists(file):
        print(f"   ✅ {file}")
    else:
        print(f"   ❌ {file} - MISSING!")

print()

# Check pages
print("📊 Checking Dashboard Pages...")
pages_dir = Path("pages")
if pages_dir.exists():
    pages = list(pages_dir.glob("*.py"))
    print(f"   ✅ Found {len(pages)} pages")
    
    # Check key pages
    key_pages = [
        "1_📊_Overview_Dashboard.py",
        "2_🤖_Load_Forecasting.py",
        "3_🔍_Tamper_Detection.py",
        "6_👥_Consumer_Segmentation.py"
    ]
    
    print("\n   Key Implemented Pages:")
    for page in key_pages:
        page_path = pages_dir / page
        if page_path.exists():
            size = page_path.stat().st_size / 1024
            print(f"      ✅ {page} ({size:.1f} KB)")
        else:
            print(f"      ❌ {page} - MISSING!")
else:
    print("   ❌ pages/ directory not found!")

print()

# Check data
print("💾 Checking Gold Layer Data...")
gold_path = Path("./iesco_gold_data")
if gold_path.exists():
    parquet_files = list(gold_path.glob("*.parquet"))
    print(f"   ✅ Found {len(parquet_files)} Parquet files")
    
    required_files = [
        "dim_meter.parquet",
        "dim_date.parquet",
        "dim_time.parquet",
        "dim_consumer_type.parquet",
        "dim_location.parquet",
        "fact_readings.parquet",
        "fact_bills.parquet",
        "fact_payments.parquet",
        "agg_monthly_consumption.parquet",
        "agg_daily_consumption.parquet",
        "agg_consumer_type_summary.parquet",
        "agg_payment_summary.parquet",
        "agg_location_summary.parquet"
    ]
    
    missing = []
    for file in required_files:
        if (gold_path / file).exists():
            size = (gold_path / file).stat().st_size / (1024 * 1024)
            print(f"      ✅ {file} ({size:.2f} MB)")
        else:
            print(f"      ❌ {file} - MISSING!")
            missing.append(file)
    
    if missing:
        print(f"\n   ⚠️  Missing {len(missing)} required files!")
else:
    print("   ❌ ./iesco_gold_data/ directory not found!")
    print("   ⚠️  Run gold_layer_pandas.py to generate data")

print()
print("="*80)
print("VERIFICATION SUMMARY")
print("="*80)

# Check if ready to run
if os.path.exists("streamlit_app.py") and os.path.exists("pages") and gold_path.exists():
    print("✅ Dashboard is ready to run!")
    print()
    print("To start the dashboard:")
    print("   1. Install dependencies: pip install -r requirements_dashboard.txt")
    print("   2. Run dashboard: streamlit run streamlit_app.py")
    print("   3. Or use: run_dashboard.bat")
else:
    print("❌ Dashboard setup incomplete!")
    print()
    print("Missing components:")
    if not os.path.exists("streamlit_app.py"):
        print("   - streamlit_app.py")
    if not os.path.exists("pages"):
        print("   - pages/ directory")
    if not gold_path.exists():
        print("   - iesco_gold_data/ directory")

print("="*80)
