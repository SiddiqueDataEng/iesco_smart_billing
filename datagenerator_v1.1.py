"""
IESCO Smart Meter Data Generator - Enhanced Version
====================================================

Generates realistic smart meter data for IESCO (Islamabad Electric Supply Company)
with the following features:

1. ZONE-BASED METER GENERATION:
   - Generates meters across 5 Circles and 19 Divisions
   - Each division has realistic min/max meter ranges
   - New meters added randomly (0.025% to 1% configurable)
   - Meters distributed over the specified time period

2. DAILY READINGS WITH DATA QUALITY ISSUES:
   - Configurable reading frequency (default: 15 minutes)
   - Realistic consumption patterns based on:
     * Tariff category (Residential/Commercial/Industrial)
     * Time of day (peak/off-peak hours)
     * Seasonality (summer/winter variations)
     * Phase type (Single/Three phase)
   - Intentional data quality issues for cleaning exercises:
     * Missing readings, negative values, zero readings
     * Abnormal spikes, voltage sags, frequency variations
     * Signal drops, battery faults

3. MONTHLY BILLS:
   - Generated based on actual meter readings
   - Realistic tariff calculations (IESCO rates)
   - Seasonal fluctuations in consumption
   - Variations by meter type, phase, and tariff

4. BILL PAYMENTS:
   - Payment status: Paid/Unpaid/Partial
   - Payment dates (before/after due date)
   - Payment methods: Bank, Mobile Apps (EasyPaisa, JazzCash), 1Bill, etc.
   - Transaction IDs
   - Outstanding amounts

Usage:
------
Interactive mode (prompts for inputs):
    python datagenerator.py

Non-interactive mode with defaults:
    python datagenerator.py --non-interactive

Custom parameters:
    python datagenerator.py --start_date 2024-01-01 --end_date 2024-12-31 --frequency 30

Author: Enhanced for IESCO Data Engineering Project
Date: 2024
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import uuid
from faker import Faker
import os
from typing import Tuple, Dict, List
import argparse

# Initialize Faker for Pakistani data
fake = Faker('en_PK')
Faker.seed(42)
np.random.seed(42)
random.seed(42)

class IESCODataGenerator:
    def __init__(self):
        self.tariff_categories = {
            'A-1a': {'name': 'General Supply (Single Phase)', 'min_load': 1, 'max_load': 5},
            'A-1b': {'name': 'General Supply (Three Phase)', 'min_load': 5, 'max_load': 30},
            'A-2a': {'name': 'Commercial (Single Phase)', 'min_load': 1, 'max_load': 5},
            'A-2b': {'name': 'Commercial (Three Phase)', 'min_load': 5, 'max_load': 50},
            'B-1': {'name': 'Industrial (Small)', 'min_load': 5, 'max_load': 25},
            'B-2': {'name': 'Industrial (Large)', 'min_load': 25, 'max_load': 500}
        }
        
        # IESCO Circles and Divisions structure
        self.circles_divisions = {
            'Islamabad Circle': {
                'Islamabad Division 1': {'min_meters': 15000, 'max_meters': 25000},
                'Islamabad Division 2': {'min_meters': 12000, 'max_meters': 20000},
                'Barakahu Division': {'min_meters': 8000, 'max_meters': 15000}
            },
            'Rawalpindi Circle': {
                'Rawat Division': {'min_meters': 10000, 'max_meters': 18000},
                'City Division': {'min_meters': 20000, 'max_meters': 35000},
                'Cantt Division (Rawalpindi)': {'min_meters': 15000, 'max_meters': 25000},
                'Satellite Town Division': {'min_meters': 12000, 'max_meters': 22000},
                'Westridge Division': {'min_meters': 10000, 'max_meters': 18000},
                'Tariqabad Division': {'min_meters': 8000, 'max_meters': 15000}
            },
            'Attock Circle': {
                'Taxila Division': {'min_meters': 12000, 'max_meters': 20000},
                'Pindigheb Division': {'min_meters': 7000, 'max_meters': 12000},
                'Attock Division': {'min_meters': 10000, 'max_meters': 18000}
            },
            'Jhelum Circle': {
                'Jhelum Division 1': {'min_meters': 10000, 'max_meters': 18000},
                'Jhelum Division 2': {'min_meters': 9000, 'max_meters': 16000},
                'Gujar Khan Division': {'min_meters': 8000, 'max_meters': 14000}
            },
            'Chakwal Circle': {
                'Chakwal Division': {'min_meters': 9000, 'max_meters': 16000},
                'Talagang Division': {'min_meters': 6000, 'max_meters': 11000},
                'Dhudial Division': {'min_meters': 5000, 'max_meters': 9000},
                'Pind Dadan Khan Division': {'min_meters': 7000, 'max_meters': 12000}
            }
        }
        
        # Legacy divisions for backward compatibility
        self.divisions = ['SATELLITE TOWN', 'GULBERG', 'WAPDA TOWN', 'SADDAR', 'RAWALPINDI CANTT']
        self.sub_divisions = ['DHOKE KALA KHAN', 'WESTRIDGE', 'CHAKLALA', 'KOHAT ROAD', 'MORGHAH']
        self.feeder_names = ['061116 QAYYUMABAD', '061117 WESTRIDGE', '061118 CHAKLALA', '061119 SADDAR', '061120 KOHAT']
        
        # Payment methods
        self.payment_methods = [
            'Bank Branch',
            'Bank ATM',
            'Bank Mobile App',
            'EasyPaisa',
            'JazzCash',
            '1Bill',
            'Online Banking',
            'IESCO Office',
            'Franchise'
        ]
        
        # Data quality issues configuration
        self.issue_probabilities = {
            'missing_reading': 0.02,      # 2% missing readings
            'negative_reading': 0.005,     # 0.5% negative readings (faulty meters)
            'zero_reading': 0.01,          # 1% zero readings
            'abnormal_spike': 0.01,         # 1% abnormal consumption spikes
            'voltage_sag': 0.015,           # 1.5% voltage sag events
            'frequency_variation': 0.01,    # 1% frequency variations
            'signal_drop': 0.02,            # 2% signal strength drops
            'battery_fault': 0.005          # 0.5% battery voltage issues
        }

    def generate_consumers_by_zone(self, start_date: str, end_date: str, 
                                   new_meter_rate_min: float = 0.025, 
                                   new_meter_rate_max: float = 1.0) -> pd.DataFrame:
        """
        Generate consumer/meter master data for all zones with realistic distribution
        New meters are added randomly between min and max rate percentage
        """
        
        consumers = []
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        total_days = (end - start).days
        
        print("\nGenerating meters by Circle and Division:")
        print("=" * 80)
        
        for circle, divisions in self.circles_divisions.items():
            print(f"\n{circle}:")
            
            for division, meter_range in divisions.items():
                # Initial number of meters at start date
                initial_meters = random.randint(meter_range['min_meters'], meter_range['max_meters'])
                
                # Calculate new meters to add over the period
                new_meter_rate = random.uniform(new_meter_rate_min, new_meter_rate_max) / 100
                total_new_meters = int(initial_meters * new_meter_rate)
                
                total_meters = initial_meters + total_new_meters
                
                print(f"  {division}: {initial_meters:,} initial + {total_new_meters:,} new = {total_meters:,} total")
                
                # Generate initial meters
                for i in range(initial_meters):
                    connection_date = fake.date_between(start_date='-10y', end_date=start)
                    consumer = self._create_consumer(connection_date, circle, division)
                    consumers.append(consumer)
                
                # Generate new meters distributed over the period
                for i in range(total_new_meters):
                    # Randomly distribute new connections throughout the period
                    days_offset = random.randint(0, total_days)
                    connection_date = start + timedelta(days=days_offset)
                    consumer = self._create_consumer(connection_date, circle, division)
                    consumers.append(consumer)
        
        print("\n" + "=" * 80)
        print(f"Total meters generated: {len(consumers):,}")
        
        return pd.DataFrame(consumers)
    
    def _create_consumer(self, connection_date, circle: str, division: str) -> Dict:
        """Helper method to create a single consumer record"""
        
        # Randomly select tariff based on realistic distribution
        tariff = random.choices(
            list(self.tariff_categories.keys()),
            weights=[0.4, 0.25, 0.15, 0.1, 0.07, 0.03],  # More residential/commercial
            k=1
        )[0]
        
        # Generate connected load based on tariff
        load_range = self.tariff_categories[tariff]
        connected_load = round(random.uniform(load_range['min_load'], load_range['max_load']), 2)
        
        # Generate consumer ID (similar to IESCO format)
        consumer_id = f"{random.randint(1000000, 9999999)}"
        
        # Generate meter number (11-14 digits)
        meter_number = f"{random.randint(10000000000, 99999999999)}"
        
        # Reference number format from PDF
        ref_no = f"11 {random.randint(10000, 99999)} {random.randint(1000000, 9999999)} U"
        
        # Generate sub-division and feeder based on division
        sub_division = random.choice(self.sub_divisions)
        feeder_name = random.choice(self.feeder_names)
        
        # Determine city based on circle
        if 'Islamabad' in circle:
            city = 'ISLAMABAD'
        elif 'Rawalpindi' in circle:
            city = 'RAWALPINDI'
        elif 'Attock' in circle:
            city = 'ATTOCK'
        elif 'Jhelum' in circle:
            city = 'JHELUM'
        else:
            city = 'CHAKWAL'
        
        consumer = {
            'consumer_id': consumer_id,
            'meter_number': meter_number,
            'reference_no': ref_no,
            'name': fake.name(),
            'father_name': fake.name_male() if random.random() > 0.3 else fake.name_female(),
            'address': f"H NO {random.randint(1, 1000)} {fake.street_name()}, {sub_division}, {city}",
            'tariff_category': tariff,
            'connection_date': connection_date,
            'connected_load_kw': connected_load,
            'circle': circle,
            'division': division,
            'sub_division': sub_division,
            'feeder_name': feeder_name,
            'phase_type': 'Single' if 'a' in tariff.lower() else 'Three',
            'meter_type': random.choice(['Smart', 'Smart', 'Smart', 'Conventional']),  # 75% smart meters
            'meter_make': random.choice(['Landis+Gyr', 'Siemens', 'Itron', 'Elster']),
            'installation_date': connection_date,
            'warranty_expiry': fake.date_between(start_date=connection_date, end_date=connection_date + timedelta(days=1825)) if random.random() > 0.2 else None,
            'last_maintenance_date': fake.date_between(start_date='-1y', end_date='today') if random.random() > 0.3 else None,
            'latitude': 33.5651 + random.uniform(-0.5, 0.5),
            'longitude': 73.0169 + random.uniform(-0.5, 0.5),
            'status': random.choice(['Active', 'Active', 'Active', 'Disconnected', 'Suspended']),
            'sanctioned_load_kw': connected_load * random.uniform(0.8, 1.2)
        }
        
        return consumer

    def generate_readings(self, 
                         meters_df: pd.DataFrame, 
                         start_date: str, 
                         end_date: str, 
                         frequency_minutes: int = 15,
                         output_path: str = None,
                         chunk_size: int = 1000) -> pd.DataFrame:
        """Generate meter readings with intentional data quality issues
        
        Args:
            meters_df: DataFrame with meter information
            start_date: Start date for readings
            end_date: End date for readings
            frequency_minutes: Reading frequency in minutes
            output_path: If provided, writes readings to CSV in chunks (memory efficient)
            chunk_size: Number of meters to process before writing to disk
        """
        
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        
        # Generate timestamp series
        timestamps = pd.date_range(start=start, end=end, freq=f'{frequency_minutes}min')
        
        readings = []
        
        # If output_path provided, write in chunks (memory efficient mode)
        write_header = True
        meters_processed = 0
        
        # Base consumption patterns (kWh per 15min)
        # Residential: 0.1-0.5 kWh per 15min (peak ~0.8)
        # Commercial: 0.3-1.0 kWh per 15min (peak ~1.5)
        # Industrial: 1.0-5.0 kWh per 15min (peak ~8.0)
        
        for _, meter in meters_df.iterrows():
            meter_number = meter['meter_number']
            consumer_id = meter['consumer_id']
            tariff = meter['tariff_category']
            
            # Set base consumption based on tariff category
            if 'A-1' in tariff:  # Residential
                base_min, base_max = 0.1, 0.5
                peak_min, peak_max = 0.5, 0.8
            elif 'A-2' in tariff:  # Commercial
                base_min, base_max = 0.3, 1.0
                peak_min, peak_max = 1.0, 1.5
            else:  # Industrial
                base_min, base_max = 1.0, 5.0
                peak_min, peak_max = 5.0, 8.0
            
            # Generate seasonal pattern (summer higher consumption)
            monthly_multiplier = {
                1: 0.7, 2: 0.7, 3: 0.8, 4: 0.9, 5: 1.2, 6: 1.4,
                7: 1.4, 8: 1.3, 9: 1.1, 10: 0.9, 11: 0.8, 12: 0.7
            }
            
            cumulative_reading = 0
            previous_reading = 0
            
            for idx, timestamp in enumerate(timestamps):
                hour = timestamp.hour
                month = timestamp.month
                
                # Peak hours: 6-10 AM and 6-11 PM
                is_peak = (6 <= hour <= 10) or (18 <= hour <= 23)
                
                # Weekend adjustment
                is_weekend = timestamp.dayofweek >= 5
                weekend_multiplier = 1.1 if is_weekend else 1.0
                
                # Base consumption with time patterns
                if is_peak:
                    base_consumption = random.uniform(peak_min, peak_max)
                else:
                    base_consumption = random.uniform(base_min, base_max)
                
                # Apply monthly (seasonal) and weekend multipliers
                consumption = base_consumption * monthly_multiplier[month] * weekend_multiplier
                
                # Add random variation
                consumption *= random.uniform(0.9, 1.1)
                
                cumulative_reading += consumption
                
                # Determine if this reading should have data quality issues
                rand_val = random.random()
                
                # Normal electrical parameters
                voltage = random.gauss(230, 10)  # 230V nominal
                current = (consumption * 1000) / voltage  # I = P/V
                frequency = random.gauss(50, 0.2)  # 50 Hz nominal
                power_factor = random.gauss(0.95, 0.03)
                temperature = random.gauss(30, 5) + (15 if hour > 12 else 0)
                signal_strength = random.gauss(-70, 10)  # dBm
                battery_voltage = random.gauss(3.7, 0.2)
                
                # INTENTIONAL DATA QUALITY ISSUES
                
                # Missing reading (completely null)
                if rand_val < self.issue_probabilities['missing_reading']:
                    continue
                
                # Negative reading (faulty meter)
                elif rand_val < self.issue_probabilities['missing_reading'] + self.issue_probabilities['negative_reading']:
                    consumption = -consumption
                    cumulative_reading = previous_reading - consumption  # Decrease cumulative
                
                # Zero reading (meter stuck)
                elif rand_val < (self.issue_probabilities['missing_reading'] + 
                               self.issue_probabilities['negative_reading'] + 
                               self.issue_probabilities['zero_reading']):
                    consumption = 0
                
                # Abnormal spike (sudden high consumption)
                elif rand_val < (self.issue_probabilities['missing_reading'] + 
                               self.issue_probabilities['negative_reading'] + 
                               self.issue_probabilities['zero_reading'] + 
                               self.issue_probabilities['abnormal_spike']):
                    consumption *= random.uniform(5, 10)
                    cumulative_reading += consumption
                
                # Voltage sag (brownout)
                elif rand_val < (self.issue_probabilities['missing_reading'] + 
                               self.issue_probabilities['negative_reading'] + 
                               self.issue_probabilities['zero_reading'] + 
                               self.issue_probabilities['abnormal_spike'] + 
                               self.issue_probabilities['voltage_sag']):
                    voltage = random.uniform(160, 200)
                
                # Frequency variation (grid instability)
                elif rand_val < (self.issue_probabilities['missing_reading'] + 
                               self.issue_probabilities['negative_reading'] + 
                               self.issue_probabilities['zero_reading'] + 
                               self.issue_probabilities['abnormal_spike'] + 
                               self.issue_probabilities['voltage_sag'] + 
                               self.issue_probabilities['frequency_variation']):
                    frequency = random.uniform(47, 53)
                
                # Signal drop (communication issue)
                elif rand_val < (self.issue_probabilities['missing_reading'] + 
                               self.issue_probabilities['negative_reading'] + 
                               self.issue_probabilities['zero_reading'] + 
                               self.issue_probabilities['abnormal_spike'] + 
                               self.issue_probabilities['voltage_sag'] + 
                               self.issue_probabilities['frequency_variation'] + 
                               self.issue_probabilities['signal_drop']):
                    signal_strength = random.uniform(-110, -90)
                
                # Battery fault (low battery)
                elif rand_val < (self.issue_probabilities['missing_reading'] + 
                               self.issue_probabilities['negative_reading'] + 
                               self.issue_probabilities['zero_reading'] + 
                               self.issue_probabilities['abnormal_spike'] + 
                               self.issue_probabilities['voltage_sag'] + 
                               self.issue_probabilities['frequency_variation'] + 
                               self.issue_probabilities['signal_drop'] + 
                               self.issue_probabilities['battery_fault']):
                    battery_voltage = random.uniform(2.5, 3.0)
                
                # Normal reading (apply all parameters normally)
                else:
                    cumulative_reading += consumption
                
                reading = {
                    'timestamp': timestamp,
                    'meter_number': meter_number,
                    'consumer_id': consumer_id,
                    'reading_kwh': round(cumulative_reading, 3),
                    'energy_consumed_kwh': round(consumption, 3),
                    'voltage_v': round(voltage, 1),
                    'current_a': round(current, 2),
                    'frequency_hz': round(frequency, 2),
                    'power_factor': round(power_factor, 3),
                    'temperature_c': round(temperature, 1),
                    'signal_strength_dbm': round(signal_strength, 1),
                    'battery_voltage_v': round(battery_voltage, 2),
                    'data_quality_flag': self._determine_quality_flag(rand_val)
                }
                
                readings.append(reading)
                previous_reading = cumulative_reading
            
            meters_processed += 1
            
            # Write chunk to disk if output_path provided and chunk size reached
            if output_path and len(readings) >= chunk_size * len(timestamps):
                chunk_df = pd.DataFrame(readings)
                chunk_df.to_csv(output_path, mode='a', header=write_header, index=False)
                write_header = False
                readings = []  # Clear memory
                print(f"  Processed {meters_processed}/{len(meters_df)} meters ({meters_processed/len(meters_df)*100:.1f}%)")
        
        # Write remaining readings if using chunked mode
        if output_path and len(readings) > 0:
            chunk_df = pd.DataFrame(readings)
            chunk_df.to_csv(output_path, mode='a', header=write_header, index=False)
            readings = []
            print(f"  Processed {meters_processed}/{len(meters_df)} meters (100%)")
        
        # If not using chunked mode, return DataFrame (for backward compatibility)
        if not output_path:
            return pd.DataFrame(readings)
        else:
            # Return empty DataFrame with message - actual data is in file
            print(f"\n  Readings written to: {output_path}")
            return pd.DataFrame()  # Empty - data is on disk
    
    def _determine_quality_flag(self, rand_val: float) -> str:
        """Determine data quality flag based on random value"""
        cumulative = 0
        for issue, prob in self.issue_probabilities.items():
            cumulative += prob
            if rand_val < cumulative:
                return issue.replace('_', ' ').title()
        return 'Normal'

    def calculate_bill(self, 
                      consumption_kwh: float, 
                      tariff: str, 
                      connected_load: float,
                      billing_month: str) -> Dict:
        """
        Calculate electricity bill based on IESCO tariff (October 2022 onward)
        Reference: https://iescobill.pk/tariff-october-01-2022/
        """
        
        # Tariff rates (as per October 2022)
        # Source: https://iescobill.pk/tariff-october-01-2022/
        
        if 'A-1' in tariff:  # Residential
            slabs = [
                (100, 5.79),    # Up to 100 units
                (200, 8.11),     # 101-200 units
                (300, 10.20),    # 201-300 units
                (400, 16.00),    # 301-400 units
                (500, 18.00),    # 401-500 units
                (float('inf'), 21.00)  # Above 500 units
            ]
            fixed_charge = 50 if connected_load < 5 else 100
            
        elif 'A-2' in tariff:  # Commercial
            slabs = [
                (100, 16.00),
                (300, 18.00),
                (float('inf'), 21.00)
            ]
            fixed_charge = 250 * connected_load
            
        elif 'B-1' in tariff:  # Small Industrial
            slabs = [
                (float('inf'), 14.00)
            ]
            fixed_charge = 200 * connected_load
            
        else:  # Large Industrial (B-2)
            slabs = [
                (float('inf'), 16.00)
            ]
            fixed_charge = 300 * connected_load
        
        # Calculate variable charges
        remaining = consumption_kwh
        variable_charges = 0
        
        for limit, rate in slabs:
            if remaining <= 0:
                break
            slab_units = min(remaining, limit)
            variable_charges += slab_units * rate
            remaining -= slab_units
        
        # Apply taxes and additional charges
        # General Sales Tax (GST) @ 18% on variable + fixed
        gst = (variable_charges + fixed_charge) * 0.18
        
        # Other charges (as per IESCO bill)
        electricity_duty = variable_charges * 0.015  # 1.5% of variable charges
        tv_fee = 35 if random.random() > 0.7 else 0  # Rs. 35 TV license fee (random)
        
        # Late payment surcharge (if applicable)
        late_payment = 0
        if random.random() > 0.9:  # 10% bills have late payment
            late_payment = (variable_charges + fixed_charge) * 0.05  # 5% penalty
        
        total_amount = variable_charges + fixed_charge + gst + electricity_duty + tv_fee + late_payment
        
        # Due date calculation (14 days from issue date)
        issue_date = pd.to_datetime(f"25 {billing_month}")  # 25th of billing month
        due_date = issue_date + timedelta(days=14)
        
        return {
            'billing_month': billing_month,
            'issue_date': issue_date.strftime('%Y-%m-%d'),
            'due_date': due_date.strftime('%Y-%m-%d'),
            'units_consumed': round(consumption_kwh, 2),
            'variable_charges': round(variable_charges, 2),
            'fixed_charges': round(fixed_charge, 2),
            'gst': round(gst, 2),
            'electricity_duty': round(electricity_duty, 2),
            'tv_fee': round(tv_fee, 2),
            'late_payment_surcharge': round(late_payment, 2),
            'total_amount': round(total_amount, 2),
            'amount_within_due_date': round(total_amount, 2),
            'amount_after_due_date': round(total_amount * 1.05, 2),  # 5% extra after due
            'tariff_applied': tariff,
            'reference_no': f"11 {random.randint(10000, 99999)} {random.randint(1000000, 9999999)} U"
        }

    def generate_monthly_bills(self, 
                              meters_df: pd.DataFrame, 
                              readings_df: pd.DataFrame,
                              start_month: str,
                              end_month: str) -> pd.DataFrame:
        """Generate monthly bills for all consumers"""
        
        start = pd.to_datetime(start_month)
        end = pd.to_datetime(end_month)
        
        # Create list of billing months
        billing_months = pd.date_range(start=start, end=end, freq='MS')
        
        bills = []
        
        for _, meter in meters_df.iterrows():
            meter_number = meter['meter_number']
            consumer_id = meter['consumer_id']
            tariff = meter['tariff_category']
            connected_load = meter['connected_load_kw']
            
            # Get readings for this meter
            meter_readings = readings_df[readings_df['meter_number'] == meter_number].copy()
            meter_readings['timestamp'] = pd.to_datetime(meter_readings['timestamp'])
            
            for billing_month in billing_months:
                month_str = billing_month.strftime('%b %y').upper()
                next_month = billing_month + pd.DateOffset(months=1)
                
                # Filter readings for this billing month
                month_readings = meter_readings[
                    (meter_readings['timestamp'] >= billing_month) & 
                    (meter_readings['timestamp'] < next_month)
                ]
                
                if len(month_readings) > 0:
                    # Calculate total consumption for the month
                    # Get first and last reading of the month
                    first_reading = month_readings.iloc[0]['reading_kwh']
                    last_reading = month_readings.iloc[-1]['reading_kwh']
                    
                    # Handle cases where readings might be negative or zero due to data quality issues
                    if last_reading < first_reading or last_reading < 0:
                        # If readings are faulty, estimate consumption from energy_consumed column
                        total_consumption = month_readings[
                            month_readings['energy_consumed_kwh'] > 0
                        ]['energy_consumed_kwh'].sum()
                    else:
                        total_consumption = last_reading - first_reading
                    
                    # Ensure consumption is not negative
                    total_consumption = max(0, total_consumption)
                    
                    # Calculate bill
                    bill = self.calculate_bill(total_consumption, tariff, connected_load, month_str)
                    
                    # Add meter and consumer info
                    bill['meter_number'] = meter_number
                    bill['consumer_id'] = consumer_id
                    bill['consumer_name'] = meter['name']
                    bill['address'] = meter['address']
                    
                    bills.append(bill)
        
        return pd.DataFrame(bills)
    
    def generate_monthly_bills_chunked(self, 
                                      meters_df: pd.DataFrame, 
                                      readings_path: str,
                                      start_month: str,
                                      end_month: str,
                                      chunk_size: int = 50000) -> pd.DataFrame:
        """Generate monthly bills by reading data in chunks (memory efficient)"""
        
        start = pd.to_datetime(start_month)
        end = pd.to_datetime(end_month)
        billing_months = pd.date_range(start=start, end=end, freq='MS')
        
        bills = []
        
        # Process each meter
        for idx, (_, meter) in enumerate(meters_df.iterrows()):
            if idx % 1000 == 0:
                print(f"  Processing bills for meter {idx}/{len(meters_df)}...")
            
            meter_number = meter['meter_number']
            consumer_id = meter['consumer_id']
            tariff = meter['tariff_category']
            connected_load = meter['connected_load_kw']
            
            # Read only this meter's readings in chunks
            meter_readings = []
            for chunk in pd.read_csv(readings_path, chunksize=chunk_size):
                meter_chunk = chunk[chunk['meter_number'] == meter_number]
                if len(meter_chunk) > 0:
                    meter_readings.append(meter_chunk)
            
            if not meter_readings:
                continue
                
            meter_readings = pd.concat(meter_readings, ignore_index=True)
            meter_readings['timestamp'] = pd.to_datetime(meter_readings['timestamp'])
            
            for billing_month in billing_months:
                month_str = billing_month.strftime('%b %y').upper()
                next_month = billing_month + pd.DateOffset(months=1)
                
                month_readings = meter_readings[
                    (meter_readings['timestamp'] >= billing_month) & 
                    (meter_readings['timestamp'] < next_month)
                ]
                
                if len(month_readings) > 0:
                    first_reading = month_readings.iloc[0]['reading_kwh']
                    last_reading = month_readings.iloc[-1]['reading_kwh']
                    
                    if last_reading < first_reading or last_reading < 0:
                        total_consumption = month_readings[
                            month_readings['energy_consumed_kwh'] > 0
                        ]['energy_consumed_kwh'].sum()
                    else:
                        total_consumption = last_reading - first_reading
                    
                    total_consumption = max(0, total_consumption)
                    
                    bill = self.calculate_bill(total_consumption, tariff, connected_load, month_str)
                    bill['meter_number'] = meter_number
                    bill['consumer_id'] = consumer_id
                    bill['consumer_name'] = meter['name']
                    bill['address'] = meter['address']
                    
                    bills.append(bill)
        
        return pd.DataFrame(bills)
    
    def generate_bill_payments(self, bills_df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate bill payment records with realistic payment patterns
        """
        
        payments = []
        
        for _, bill in bills_df.iterrows():
            # Determine payment status (85% paid, 15% unpaid)
            is_paid = random.random() < 0.85
            
            if is_paid:
                due_date = pd.to_datetime(bill['due_date'])
                issue_date = pd.to_datetime(bill['issue_date'])
                
                # Payment timing distribution
                # 60% pay before due date
                # 25% pay within 7 days after due date
                # 15% pay late (8-30 days after due date)
                
                rand_val = random.random()
                
                if rand_val < 0.60:  # Before due date
                    days_before = random.randint(1, 14)
                    payment_date = due_date - timedelta(days=days_before)
                    paid_amount = bill['amount_within_due_date']
                    
                elif rand_val < 0.85:  # Within 7 days after due date
                    days_after = random.randint(1, 7)
                    payment_date = due_date + timedelta(days=days_after)
                    paid_amount = bill['amount_after_due_date']
                    
                else:  # Late payment (8-30 days after)
                    days_after = random.randint(8, 30)
                    payment_date = due_date + timedelta(days=days_after)
                    # Additional late charges
                    paid_amount = bill['amount_after_due_date'] * random.uniform(1.0, 1.1)
                
                # Ensure payment date is not before issue date
                if payment_date < issue_date:
                    payment_date = issue_date + timedelta(days=random.randint(1, 5))
                
                # Select payment method based on realistic distribution
                payment_method = random.choices(
                    self.payment_methods,
                    weights=[0.15, 0.10, 0.20, 0.15, 0.12, 0.08, 0.10, 0.05, 0.05],
                    k=1
                )[0]
                
                # Generate transaction ID
                if payment_method in ['EasyPaisa', 'JazzCash', '1Bill']:
                    transaction_id = f"EP{random.randint(1000000000, 9999999999)}"
                elif 'Bank' in payment_method or 'Online' in payment_method:
                    transaction_id = f"BNK{random.randint(100000000, 999999999)}"
                else:
                    transaction_id = f"CSH{random.randint(10000000, 99999999)}"
                
                payment_status = 'Paid'
                
                # Small chance of partial payment
                if random.random() < 0.05:
                    paid_amount = paid_amount * random.uniform(0.5, 0.95)
                    payment_status = 'Partial'
                
            else:
                # Unpaid bill
                payment_date = None
                paid_amount = 0
                payment_method = None
                transaction_id = None
                payment_status = 'Unpaid'
            
            payment = {
                'consumer_id': bill['consumer_id'],
                'meter_number': bill['meter_number'],
                'billing_month': bill['billing_month'],
                'bill_amount': bill['total_amount'],
                'due_date': bill['due_date'],
                'payment_status': payment_status,
                'payment_date': payment_date.strftime('%Y-%m-%d') if payment_date else None,
                'paid_amount': round(paid_amount, 2) if is_paid else 0,
                'payment_method': payment_method,
                'transaction_id': transaction_id,
                'outstanding_amount': round(bill['total_amount'] - (paid_amount if is_paid else 0), 2)
            }
            
            payments.append(payment)
        
        return pd.DataFrame(payments)

    def generate_all_data(self, 
                         start_date: str = None,
                         end_date: str = None,
                         reading_frequency: int = None,
                         new_meter_rate_min: float = 0.025,
                         new_meter_rate_max: float = 1.0,
                         output_dir: str = './iesco_data',
                         interactive: bool = True):
        """
        Generate all datasets with interactive prompts
        """
        
        # Interactive prompts
        if interactive:
            print("\n" + "="*80)
            print("IESCO DATA GENERATOR - Enhanced Version")
            print("="*80)
            
            if not start_date:
                start_input = input("\nEnter start date (YYYY-MM-DD) [default: 2024-01-01]: ").strip()
                start_date = start_input if start_input else '2024-01-01'
            
            if not end_date:
                end_input = input("Enter end date (YYYY-MM-DD) [default: 2024-12-31]: ").strip()
                end_date = end_input if end_input else '2024-12-31'
            
            if not reading_frequency:
                freq_input = input("Enter reading frequency in minutes [default: 15]: ").strip()
                reading_frequency = int(freq_input) if freq_input else 15
            
            print(f"\nNew meter addition rate: {new_meter_rate_min}% to {new_meter_rate_max}% per zone")
            change_rate = input("Change rate? (y/n) [default: n]: ").strip().lower()
            if change_rate == 'y':
                min_rate = input(f"Enter minimum rate % [default: {new_meter_rate_min}]: ").strip()
                max_rate = input(f"Enter maximum rate % [default: {new_meter_rate_max}]: ").strip()
                new_meter_rate_min = float(min_rate) if min_rate else new_meter_rate_min
                new_meter_rate_max = float(max_rate) if max_rate else new_meter_rate_max
        else:
            start_date = start_date or '2024-01-01'
            end_date = end_date or '2024-12-31'
            reading_frequency = reading_frequency or 15
        
        print(f"\n{'='*80}")
        print("GENERATION PARAMETERS:")
        print(f"{'='*80}")
        print(f"Period: {start_date} to {end_date}")
        print(f"Reading Frequency: {reading_frequency} minutes")
        print(f"New Meter Rate: {new_meter_rate_min}% - {new_meter_rate_max}%")
        print(f"Output Directory: {output_dir}")
        
        # Generate meters by zone
        print(f"\n{'='*80}")
        print("STEP 1: Generating Meters by Circle and Division")
        print(f"{'='*80}")
        meters_df = self.generate_consumers_by_zone(
            start_date, end_date, new_meter_rate_min, new_meter_rate_max
        )
        
        # Generate readings
        print(f"\n{'='*80}")
        print("STEP 2: Generating Daily Meter Readings")
        print(f"{'='*80}")
        print(f"Generating readings at {reading_frequency}-minute intervals...")
        print("Writing readings directly to disk in chunks to save memory...")
        
        # Create output directory first
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        readings_path = os.path.join(output_dir, f'readings_{timestamp}.csv')
        
        # Generate readings with chunked writing (memory efficient)
        self.generate_readings(meters_df, start_date, end_date, reading_frequency, 
                              output_path=readings_path, chunk_size=100)
        
        # Load readings back for bill generation (read in chunks if needed)
        print("\nLoading readings for bill generation...")
        try:
            readings_df = pd.read_csv(readings_path)
        except MemoryError:
            print("Warning: Readings file too large to load entirely. Using chunked processing for bills...")
            readings_df = None  # Will process in chunks
        
        # Generate bills
        print(f"\n{'='*80}")
        print("STEP 3: Generating Monthly Bills")
        print(f"{'='*80}")
        
        if readings_df is not None:
            bills_df = self.generate_monthly_bills(meters_df, readings_df, start_date, end_date)
        else:
            # For very large datasets, generate bills by processing readings in chunks
            print("Processing bills with chunked reading data...")
            bills_df = self.generate_monthly_bills_chunked(meters_df, readings_path, start_date, end_date)
        
        # Generate payments
        print(f"\n{'='*80}")
        print("STEP 4: Generating Bill Payments")
        print(f"{'='*80}")
        payments_df = self.generate_bill_payments(bills_df)
        
        # Save to CSV (readings already saved)
        meters_path = os.path.join(output_dir, f'meters_{timestamp}.csv')
        bills_path = os.path.join(output_dir, f'bills_{timestamp}.csv')
        payments_path = os.path.join(output_dir, f'payments_{timestamp}.csv')
        
        print("\nSaving data to CSV files...")
        meters_df.to_csv(meters_path, index=False)
        bills_df.to_csv(bills_path, index=False)
        payments_df.to_csv(payments_path, index=False)
        # Note: readings already saved during generation
        
        # Print summary
        print("\n" + "="*80)
        print("DATA GENERATION COMPLETE")
        print("="*80)
        
        print(f"\n1. METERS DATA: {meters_path}")
        print(f"   Shape: {meters_df.shape}")
        print(f"   Total Meters: {len(meters_df):,}")
        print(f"   Circles: {meters_df['circle'].nunique()}")
        print(f"   Divisions: {meters_df['division'].nunique()}")
        
        print(f"\n2. READINGS DATA: {readings_path}")
        # Get file size instead of loading into memory
        import os as os_module
        file_size_mb = os_module.path.getsize(readings_path) / (1024 * 1024)
        print(f"   File Size: {file_size_mb:.2f} MB")
        
        if readings_df is not None and len(readings_df) > 0:
            print(f"   Shape: {readings_df.shape}")
            print(f"   Date Range: {readings_df['timestamp'].min()} to {readings_df['timestamp'].max()}")
            print(f"   Total Readings: {len(readings_df):,}")
            
            # Data quality statistics
            print(f"\n   Data Quality Issues:")
            quality_counts = readings_df['data_quality_flag'].value_counts()
            for flag, count in quality_counts.items():
                percentage = (count / len(readings_df)) * 100
                print(f"     - {flag}: {count:,} ({percentage:.2f}%)")
        else:
            print(f"   (Data written directly to disk - too large to load into memory)")
            print(f"   Use pandas.read_csv() with chunksize parameter to process")
        
        print(f"\n3. BILLS DATA: {bills_path}")
        print(f"   Shape: {bills_df.shape}")
        print(f"   Billing Months: {bills_df['billing_month'].nunique()}")
        print(f"   Total Bills: {len(bills_df):,}")
        print(f"   Average Bill: Rs. {bills_df['total_amount'].mean():,.2f}")
        print(f"   Total Revenue: Rs. {bills_df['total_amount'].sum():,.2f}")
        
        print(f"\n4. PAYMENTS DATA: {payments_path}")
        print(f"   Shape: {payments_df.shape}")
        print(f"   Total Payments: {len(payments_df):,}")
        
        payment_stats = payments_df['payment_status'].value_counts()
        for status, count in payment_stats.items():
            percentage = (count / len(payments_df)) * 100
            print(f"     - {status}: {count:,} ({percentage:.2f}%)")
        
        print(f"\n   Payment Methods:")
        method_stats = payments_df[payments_df['payment_method'].notna()]['payment_method'].value_counts()
        for method, count in method_stats.head(5).items():
            percentage = (count / len(payments_df[payments_df['payment_method'].notna()])) * 100
            print(f"     - {method}: {count:,} ({percentage:.2f}%)")
        
        print(f"\n   Collection Rate: {(payments_df['paid_amount'].sum() / bills_df['total_amount'].sum() * 100):.2f}%")
        print(f"   Total Collected: Rs. {payments_df['paid_amount'].sum():,.2f}")
        print(f"   Total Outstanding: Rs. {payments_df['outstanding_amount'].sum():,.2f}")
        
        print("\n" + "="*80)
        
        return meters_df, readings_df, bills_df, payments_df


def main():
    parser = argparse.ArgumentParser(
        description='Generate IESCO Smart Meter Data with realistic patterns',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Interactive mode (prompts for inputs)
  python datagenerator.py
  
  # Command-line mode with defaults
  python datagenerator.py --non-interactive
  
  # Custom parameters
  python datagenerator.py --start_date 2024-01-01 --end_date 2024-12-31 --frequency 30
  
  # Custom new meter rate
  python datagenerator.py --new_meter_min 0.5 --new_meter_max 2.0
        """
    )
    
    parser.add_argument('--start_date', type=str, default=None,
                       help='Start date for readings (YYYY-MM-DD) [default: 2024-01-01]')
    parser.add_argument('--end_date', type=str, default=None,
                       help='End date for readings (YYYY-MM-DD) [default: 2024-12-31]')
    parser.add_argument('--frequency', type=int, default=None,
                       help='Reading frequency in minutes [default: 15]')
    parser.add_argument('--new_meter_min', type=float, default=0.025,
                       help='Minimum new meter rate percentage [default: 0.025]')
    parser.add_argument('--new_meter_max', type=float, default=1.0,
                       help='Maximum new meter rate percentage [default: 1.0]')
    parser.add_argument('--output_dir', type=str, default='./iesco_data',
                       help='Output directory for CSV files [default: ./iesco_data]')
    parser.add_argument('--non-interactive', action='store_true',
                       help='Run in non-interactive mode (use defaults/args only)')
    
    args = parser.parse_args()
    
    generator = IESCODataGenerator()
    
    meters, readings, bills, payments = generator.generate_all_data(
        start_date=args.start_date,
        end_date=args.end_date,
        reading_frequency=args.frequency,
        new_meter_rate_min=args.new_meter_min,
        new_meter_rate_max=args.new_meter_max,
        output_dir=args.output_dir,
        interactive=not args.non_interactive
    )


if __name__ == "__main__":
    main()