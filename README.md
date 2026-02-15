# IESCO Data Generator - Enhanced Version

## Overview

This enhanced data generator creates realistic IESCO (Islamabad Electric Supply Company) smart meter data for data engineering and analytics projects.

## ⚠️ Memory Optimization for Large Datasets

This generator now uses **chunked processing** to handle large datasets efficiently:
- Writes readings directly to disk instead of loading all into memory
- Can generate data for 240K+ meters over a full year without memory issues
- Automatically falls back to chunked processing when needed
- See [MEMORY_OPTIMIZATION.md](MEMORY_OPTIMIZATION.md) for details

**Recommended for large datasets:**
- Use 30 or 60 minute reading intervals instead of 15 minutes
- Generate data in smaller time periods if needed
- The script will handle memory automatically

## Key Features

### 1. Zone-Based Meter Generation
- **5 Circles**: Islamabad, Rawalpindi, Attock, Jhelum, Chakwal
- **19 Divisions** across all circles with realistic meter distributions
- Each division has min/max meter ranges based on actual IESCO structure
- New meters added randomly between 0.025% to 1% (configurable)
- Meters distributed throughout the specified time period

### 2. Realistic Daily Readings
- Configurable reading frequency (default: 15 minutes)
- Consumption patterns based on:
  - **Tariff Category**: Residential (A-1), Commercial (A-2), Industrial (B-1, B-2)
  - **Time of Day**: Peak hours (6-10 AM, 6-11 PM) vs off-peak
  - **Seasonality**: Higher consumption in summer (May-August)
  - **Day Type**: Weekend vs weekday patterns
  - **Phase Type**: Single vs Three phase

### 3. Intentional Data Quality Issues
For data cleaning exercises, the generator includes:
- Missing readings (2%)
- Negative readings (0.5%)
- Zero readings (1%)
- Abnormal consumption spikes (1%)
- Voltage sags (1.5%)
- Frequency variations (1%)
- Signal strength drops (2%)
- Battery faults (0.5%)

### 4. Monthly Bill Generation
- Calculated from actual meter readings
- IESCO tariff rates (October 2022 onwards)
- Includes:
  - Variable charges (slab-based)
  - Fixed charges
  - GST (18%)
  - Electricity duty (1.5%)
  - TV license fee
  - Late payment surcharges

### 5. Bill Payment Records
- **Payment Status**: Paid (85%), Unpaid (15%), Partial (5% of paid)
- **Payment Timing**:
  - 60% pay before due date
  - 25% pay within 7 days after due date
  - 15% pay late (8-30 days after)
- **Payment Methods**:
  - Bank Branch (15%)
  - Bank Mobile App (20%)
  - EasyPaisa (15%)
  - JazzCash (12%)
  - 1Bill (8%)
  - Online Banking (10%)
  - Bank ATM (10%)
  - IESCO Office (5%)
  - Franchise (5%)
- Transaction IDs for each payment
- Outstanding amount tracking

## Usage

### Interactive Mode (Recommended)
```bash
python datagenerator.py
```
The script will prompt you for:
- Start date (default: 2024-01-01)
- End date (default: 2024-12-31)
- Reading frequency in minutes (default: 15)
- New meter addition rate (optional)

### Non-Interactive Mode
```bash
python datagenerator.py --non-interactive
```
Uses all default values.

### Custom Parameters
```bash
python datagenerator.py \
  --start_date 2024-01-01 \
  --end_date 2024-12-31 \
  --frequency 30 \
  --new_meter_min 0.5 \
  --new_meter_max 2.0 \
  --output_dir ./my_data
```

### Command-Line Arguments
- `--start_date`: Start date (YYYY-MM-DD) [default: 2024-01-01]
- `--end_date`: End date (YYYY-MM-DD) [default: 2024-12-31]
- `--frequency`: Reading interval in minutes [default: 15]
- `--new_meter_min`: Minimum new meter rate % [default: 0.025]
- `--new_meter_max`: Maximum new meter rate % [default: 1.0]
- `--output_dir`: Output directory [default: ./iesco_data]
- `--non-interactive`: Skip prompts, use defaults

## Output Files

The generator creates 4 CSV files with timestamps:

### 1. meters_YYYYMMDD_HHMMSS.csv
Consumer and meter master data with columns:
- consumer_id, meter_number, reference_no
- name, father_name, address
- tariff_category, connection_date
- connected_load_kw, sanctioned_load_kw
- circle, division, sub_division, feeder_name
- phase_type, meter_type, meter_make
- installation_date, warranty_expiry
- latitude, longitude, status

### 2. readings_YYYYMMDD_HHMMSS.csv
Time-series meter readings with columns:
- timestamp, meter_number, consumer_id
- reading_kwh, energy_consumed_kwh
- voltage_v, current_a, frequency_hz
- power_factor, temperature_c
- signal_strength_dbm, battery_voltage_v
- data_quality_flag

### 3. bills_YYYYMMDD_HHMMSS.csv
Monthly electricity bills with columns:
- meter_number, consumer_id, consumer_name, address
- billing_month, issue_date, due_date
- units_consumed
- variable_charges, fixed_charges
- gst, electricity_duty, tv_fee
- late_payment_surcharge
- total_amount, amount_within_due_date, amount_after_due_date
- tariff_applied, reference_no

### 4. payments_YYYYMMDD_HHMMSS.csv
Bill payment records with columns:
- consumer_id, meter_number
- billing_month, bill_amount, due_date
- payment_status (Paid/Unpaid/Partial)
- payment_date, paid_amount
- payment_method, transaction_id
- outstanding_amount

## IESCO Circle and Division Structure

### Islamabad Circle
- Islamabad Division 1: 15,000 - 25,000 meters
- Islamabad Division 2: 12,000 - 20,000 meters
- Barakahu Division: 8,000 - 15,000 meters

### Rawalpindi Circle
- Rawat Division: 10,000 - 18,000 meters
- City Division: 20,000 - 35,000 meters
- Cantt Division: 15,000 - 25,000 meters
- Satellite Town Division: 12,000 - 22,000 meters
- Westridge Division: 10,000 - 18,000 meters
- Tariqabad Division: 8,000 - 15,000 meters

### Attock Circle
- Taxila Division: 12,000 - 20,000 meters
- Pindigheb Division: 7,000 - 12,000 meters
- Attock Division: 10,000 - 18,000 meters

### Jhelum Circle
- Jhelum Division 1: 10,000 - 18,000 meters
- Jhelum Division 2: 9,000 - 16,000 meters
- Gujar Khan Division: 8,000 - 14,000 meters

### Chakwal Circle
- Chakwal Division: 9,000 - 16,000 meters
- Talagang Division: 6,000 - 11,000 meters
- Dhudial Division: 5,000 - 9,000 meters
- Pind Dadan Khan Division: 7,000 - 12,000 meters

## Data Volume Estimates

For 1 year of data (2024-01-01 to 2024-12-31):

| Reading Frequency | Total Meters | Readings per Meter | Total Readings | Approx File Size |
|-------------------|--------------|-------------------|----------------|------------------|
| 15 minutes        | ~200,000     | ~35,040          | ~7 billion     | ~500 GB          |
| 30 minutes        | ~200,000     | ~17,520          | ~3.5 billion   | ~250 GB          |
| 60 minutes        | ~200,000     | ~8,760           | ~1.75 billion  | ~125 GB          |

**Note**: For testing, use a shorter date range or higher frequency interval.

## Dependencies

```bash
pip install pandas numpy faker
```

## Example Output Summary

```
================================================================================
DATA GENERATION COMPLETE
================================================================================

1. METERS DATA: ./iesco_data/meters_20260215_120000.csv
   Shape: (198,450, 22)
   Total Meters: 198,450
   Circles: 5
   Divisions: 19

2. READINGS DATA: ./iesco_data/readings_20260215_120000.csv
   Shape: (6,954,756,000, 13)
   Date Range: 2024-01-01 00:00:00 to 2024-12-31 23:45:00
   Total Readings: 6,954,756,000

   Data Quality Issues:
     - Normal: 6,815,421,120 (98.00%)
     - Missing Reading: 139,095,120 (2.00%)
     - Signal Drop: 69,547,560 (1.00%)
     ...

3. BILLS DATA: ./iesco_data/bills_20260215_120000.csv
   Shape: (2,381,400, 17)
   Billing Months: 12
   Total Bills: 2,381,400
   Average Bill: Rs. 3,245.67
   Total Revenue: Rs. 7,726,542,318.00

4. PAYMENTS DATA: ./iesco_data/payments_20260215_120000.csv
   Shape: (2,381,400, 11)
   Total Payments: 2,381,400
     - Paid: 2,024,190 (85.00%)
     - Unpaid: 357,210 (15.00%)

   Payment Methods:
     - Bank Mobile App: 404,838 (20.00%)
     - Bank Branch: 303,628 (15.00%)
     - EasyPaisa: 303,628 (15.00%)
     ...

   Collection Rate: 85.23%
   Total Collected: Rs. 6,582,773,170.00
   Total Outstanding: Rs. 1,143,769,148.00
```

## Use Cases

1. **Data Engineering Projects**: Practice ETL pipelines with realistic data
2. **Data Quality Analysis**: Identify and fix data quality issues
3. **Analytics & BI**: Build dashboards and reports
4. **Machine Learning**: Predict consumption, detect anomalies, forecast demand
5. **Database Design**: Design and optimize database schemas
6. **Performance Testing**: Test systems with large-scale data

## Notes

- All data is synthetic and generated using Faker library
- Consumption patterns are modeled on typical Pakistani electricity usage
- Tariff rates are based on IESCO's October 2022 tariff structure
- Data quality issues are intentionally introduced for learning purposes
- For production use, adjust parameters and validation rules accordingly

## License

This tool is for educational and testing purposes only.
