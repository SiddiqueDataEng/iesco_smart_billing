# IESCO Smart Billing - Complete Data Pipeline

Complete end-to-end data pipeline for IESCO smart meter billing system with Bronze, Silver, and Gold layers.

## Quick Start

### 1. Generate Data (Bronze Layer)
```bash
python datagenerator_v2.0_parallel.py
```
- Generates 100 meters with hourly readings for 2024
- Output: `./iesco_complete_data/` (CSV format)
- Time: ~25 seconds

### 2. Clean Data (Silver Layer)
```bash
python silver_clean.py
```
- Cleans and validates Bronze Layer data
- Output: `./iesco_silver_data/` (Parquet format)
- Time: ~1 minute

### 3. Create Analytics Layer (Gold Layer)
```bash
python gold_layer_pandas.py
```
- Creates Star Schema with dimensions and facts
- Output: `./iesco_gold_data/` (Parquet format)
- Time: ~1 minute

### 4. Verify Results
```bash
python test_silver_layer.py
python test_gold_layer.py
```

## Project Structure

### Essential Files
```
📁 iesco_smart_billing/
├── 📄 README.md                          # This file
├── 📄 QUICK_START_V2.md                  # Detailed quick start guide
├── 📄 COMPLETE_PIPELINE_STATUS.md        # Complete pipeline status
│
├── 🔧 Data Generation (Bronze Layer)
│   └── datagenerator_v2.0_parallel.py    # Main data generator
│
├── 🧹 Data Cleaning (Silver Layer)
│   ├── silver_clean.py                   # Main cleaning script
│   ├── test_silver_layer.py              # Verification script
│   └── SILVER_LAYER_README.md            # Documentation
│
├── 📊 Analytics Layer (Gold Layer)
│   ├── gold_layer_pandas.py              # Main Gold Layer script
│   ├── test_gold_layer.py                # Verification script
│   └── GOLD_LAYER_README.md              # Documentation
│
├── 📁 Data Directories
│   ├── iesco_complete_data/              # Bronze Layer (CSV)
│   ├── iesco_silver_data/                # Silver Layer (Parquet)
│   └── iesco_gold_data/                  # Gold Layer (Parquet)
│
├── 📚 Documentation
│   ├── ARCHITECTURE_V2.md                # Architecture overview
│   ├── INDEX_V2.md                       # Documentation index
│   ├── VISUAL_FLOW_V2.md                 # Visual data flow
│   ├── V2_RELEASE_NOTES.md               # Release notes
│   └── FIX_COMPLETE_SUMMARY.md           # Recent fixes
│
└── 📁 Extra Files
    ├── extra/                            # Archived/debug files
    ├── older_versions/                   # Previous versions
    └── docs/                             # Additional documentation
```

## Data Pipeline Overview

```
Bronze Layer (Raw)          Silver Layer (Clean)       Gold Layer (Analytics)
==================          ====================       ======================
CSV Files                   Parquet Files              Star Schema
293 MB                      10.98 MB                   11.73 MB

meters.csv                  meters.parquet             5 Dimension Tables:
readings.csv                readings.parquet           - dim_meter
bills.csv                   bills.parquet              - dim_date
payments.csv                payments.parquet           - dim_time
                                                       - dim_consumer_type
                                                       - dim_location

                                                       3 Fact Tables:
                                                       - fact_readings (842K)
                                                       - fact_bills (1,188)
                                                       - fact_payments (1,188)

                                                       5 Aggregate Tables:
                                                       - agg_monthly_consumption
                                                       - agg_daily_consumption
                                                       - agg_consumer_type_summary
                                                       - agg_payment_summary
                                                       - agg_location_summary
```

## Features

### Data Generation
- ✅ Parallel processing (4 workers)
- ✅ 100 meters across multiple districts
- ✅ 842,292 hourly readings for 2024
- ✅ 1,188 monthly bills
- ✅ 1,188 payments (realistic paid/unpaid scenarios)
- ✅ 45 consumer types
- ✅ Realistic grid behavior (load shedding, theft, failures)

### Data Cleaning
- ✅ Duplicate removal
- ✅ Missing value handling
- ✅ Outlier detection and correction
- ✅ Referential integrity validation
- ✅ Anomaly flagging
- ✅ 91% compression (CSV → Parquet)

### Analytics Layer
- ✅ Star Schema dimensional model
- ✅ 5 dimension tables
- ✅ 3 fact tables
- ✅ 5 pre-aggregated tables
- ✅ Ready for BI tools (Power BI, Tableau)
- ✅ Optimized for analytics queries

## Requirements

- Python 3.12+
- pandas
- numpy
- pyarrow (for Parquet)
- faker
- tqdm

Install dependencies:
```bash
pip install pandas numpy pyarrow faker tqdm
```

## Usage Examples

### Load Silver Layer Data
```python
import pandas as pd

# Load cleaned data
meters = pd.read_parquet('./iesco_silver_data/meters.parquet')
readings = pd.read_parquet('./iesco_silver_data/readings.parquet')
bills = pd.read_parquet('./iesco_silver_data/bills.parquet')
```

### Query Gold Layer
```python
# Load dimension and fact tables
dim_meter = pd.read_parquet('./iesco_gold_data/dim_meter.parquet')
fact_readings = pd.read_parquet('./iesco_gold_data/fact_readings.parquet')

# Load pre-aggregated data
monthly_consumption = pd.read_parquet('./iesco_gold_data/agg_monthly_consumption.parquet')
consumer_summary = pd.read_parquet('./iesco_gold_data/agg_consumer_type_summary.parquet')
```

## Performance

| Layer | Size | Records | Format | Time |
|-------|------|---------|--------|------|
| Bronze | 293 MB | 842K+ | CSV | 25 sec |
| Silver | 10.98 MB | 842K+ | Parquet | 1 min |
| Gold | 11.73 MB | 13 tables | Parquet | 1 min |

**Total Pipeline**: ~3 minutes end-to-end

## Recent Updates

### ✅ meter_number Mismatch - FIXED
- **Issue**: Readings had different meter_numbers than meters table
- **Fix**: Updated data generator to use correct field name
- **Result**: 100% referential integrity, all aggregate tables populated
- **Details**: See `FIX_COMPLETE_SUMMARY.md`

## Documentation

- **Quick Start**: `QUICK_START_V2.md`
- **Architecture**: `ARCHITECTURE_V2.md`
- **Complete Status**: `COMPLETE_PIPELINE_STATUS.md`
- **Visual Flow**: `VISUAL_FLOW_V2.md`
- **Documentation Index**: `INDEX_V2.md`
- **Silver Layer**: `SILVER_LAYER_README.md`
- **Gold Layer**: `GOLD_LAYER_README.md`
- **Recent Fixes**: `FIX_COMPLETE_SUMMARY.md`

## Use Cases

- Consumer behavior analysis
- Consumption pattern analysis (hourly, daily, monthly)
- Payment behavior tracking
- Revenue forecasting
- Load forecasting and grid planning
- Anomaly detection
- Customer segmentation
- BI dashboards and reports
- Machine learning pipelines

## Support

For issues or questions: Muhammad Siddique +92 331 5868725

## License

This project is for educational and demonstration purposes.
