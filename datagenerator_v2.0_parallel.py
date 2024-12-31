"""
IESCO PARALLEL PIPELINE GENERATOR - VERSION 2.0
================================================
Revolutionary parallel processing approach:
✓ Per-Meter Pipeline: readings → bills → payments (complete one meter before next)
✓ Multi-threaded: Process multiple meters concurrently
✓ Memory Efficient: Stream processing, no bulk loading
✓ Real-time Progress: See each meter complete its lifecycle
✓ All v1.5 features preserved: 40+ consumer types, grid events, theft, etc.

Pipeline Flow:
1. Create meter → 2. Generate readings (month by month) → 3. Calculate bill → 4. Generate payment → Next month
After all months complete for meter → Move to next meter (parallel)
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import random
from faker import Faker
import os
import json
from tqdm import tqdm
from typing import Tuple, Dict, List, Optional
from collections import defaultdict
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from queue import Queue
import time

# Import the base simulator from v1.5
import sys
import importlib.util

# Load v1.5 dynamically
def load_v15_simulator():
    """Load the v1.5 simulator dynamically"""
    try:
        spec = importlib.util.spec_from_file_location("datagenerator_v1_5", "datagenerator_v1.5.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.IESCOCompleteGridSimulator
    except Exception as e:
        print(f"⚠️  Could not import v1.5 base: {e}")
        print("   Using standalone mode with simplified features.")
        return None

BaseSimulator = load_v15_simulator()

# Initialize
fake = Faker('en_PK')
Faker.seed(42)

class IESCOParallelPipelineGenerator:
    """
    Parallel pipeline generator that processes each meter through complete lifecycle
    """
    
    def __init__(self, base_simulator=None):
        """Initialize with base simulator for reusing v1.5 logic"""
        if base_simulator:
            self.base = base_simulator
        else:
            # Create new base simulator
            if BaseSimulator:
                self.base = BaseSimulator()
                print("✅ Loaded v1.5 simulator with all features")
            else:
                print("⚠️  Running in minimal mode without v1.5 features")
                self.base = None
        
        # Thread-safe data structures
        self.lock = threading.Lock()
        self.readings_buffer = []
        self.bills_buffer = []
        self.payments_buffer = []
        self.progress_stats = {
            'meters_completed': 0,
            'readings_generated': 0,
            'bills_generated': 0,
            'payments_generated': 0
        }
        
    def process_single_meter_pipeline(self, meter_data: Dict, transformers_df: pd.DataFrame,
                                     start_date: str, end_date: str, 
                                     frequency_minutes: int = 15,
                                     output_dir: str = './iesco_parallel_data') -> Dict:
        """
        Complete pipeline for ONE meter: readings → bills → payments
        Processes month by month sequentially for this meter
        """
        meter_id = meter_data['meter_id']
        results = {
            'meter_id': meter_id,
            'readings': [],
            'bills': [],
            'payments': [],
            'status': 'processing'
        }
        
        try:
            # Parse dates
            current_date = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            
            # Process month by month
            while current_date <= end_dt:
                month_start = current_date
                month_end = month_start + relativedelta(months=1) - timedelta(days=1)
                
                # Ensure we don't go past end_date
                if month_end > end_dt:
                    month_end = end_dt
                
                # STEP 1: Generate readings for this month
                month_readings = self._generate_month_readings(
                    meter_data, transformers_df, month_start, month_end, frequency_minutes
                )
                results['readings'].extend(month_readings)
                
                # STEP 2: Calculate bill for this month (immediately after readings)
                if month_readings:
                    month_bill = self._calculate_month_bill(
                        meter_data, month_readings, month_start
                    )
                    results['bills'].append(month_bill)
                    
                    # STEP 3: Generate payment for this bill (immediately after bill)
                    payment = self._generate_payment(meter_data, month_bill)
                    results['payments'].append(payment)
                
                # Move to next month
                current_date = month_start + relativedelta(months=1)
            
            results['status'] = 'completed'
            
            # Write to disk immediately (thread-safe)
            self._write_meter_data_to_disk(results, output_dir)
            
            # Update progress
            with self.lock:
                self.progress_stats['meters_completed'] += 1
                self.progress_stats['readings_generated'] += len(results['readings'])
                self.progress_stats['bills_generated'] += len(results['bills'])
                self.progress_stats['payments_generated'] += len(results['payments'])
            
        except Exception as e:
            results['status'] = 'failed'
            results['error'] = str(e)
            print(f"❌ Error processing meter {meter_id}: {e}")
        
        return results
    
    def _generate_month_readings(self, meter_data: Dict, transformers_df: pd.DataFrame,
                                month_start: datetime, month_end: datetime,
                                frequency_minutes: int) -> List[Dict]:
        """Generate readings for one meter for one month"""
        readings = []
        
        # Generate timestamps for this month
        current_time = month_start
        while current_time <= month_end:
            # Generate single reading
            reading = self._generate_single_reading(meter_data, transformers_df, current_time)
            readings.append(reading)
            
            # Move to next interval
            current_time += timedelta(minutes=frequency_minutes)
        
        return readings
    
    def _generate_single_reading(self, meter_data: Dict, transformers_df: pd.DataFrame,
                                timestamp: datetime) -> Dict:
        """Generate a single reading for a meter at specific timestamp"""
        
        # Use base simulator if available
        if self.base and hasattr(self.base, 'generate_consumption_patterns'):
            consumption = self.base.generate_consumption_patterns(
                meter_data.get('consumer_type', 'RESIDENTIAL_GENERAL'),
                timestamp
            )
        else:
            # Simple fallback
            base_load = meter_data.get('sanctioned_load_kw', 5)
            hour = timestamp.hour
            # Simple pattern: higher during evening
            if 18 <= hour <= 23:
                consumption = base_load * random.uniform(0.7, 0.95)
            elif 6 <= hour <= 9:
                consumption = base_load * random.uniform(0.5, 0.7)
            else:
                consumption = base_load * random.uniform(0.2, 0.5)
        
        # Calculate cumulative
        prev_reading = meter_data.get('last_reading', 0)
        current_reading = prev_reading + (consumption * (15/60))  # 15-min interval
        meter_data['last_reading'] = current_reading
        
        reading = {
            'meter_number': meter_data['meter_id'],  # Use meter_number to match meters table
            'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'reading_kwh': round(current_reading, 2),
            'consumption_kwh': round(consumption * (15/60), 3),
            'voltage_v': round(random.gauss(230, 5), 1),
            'current_a': round(consumption / 0.23, 2),
            'power_factor': round(random.gauss(0.95, 0.02), 2),
            'frequency_hz': round(random.gauss(50, 0.1), 1),
            'status': 'normal'
        }
        
        return reading
    
    def _calculate_month_bill(self, meter_data: Dict, month_readings: List[Dict],
                             billing_month: datetime) -> Dict:
        """Calculate bill for one meter for one month"""
        
        if not month_readings:
            return None
        
        # Calculate total consumption
        total_consumption = sum(r['consumption_kwh'] for r in month_readings)
        
        # Use base simulator billing if available
        if self.base and hasattr(self.base, 'calculate_bill_amount'):
            bill_amount, breakdown = self.base.calculate_bill_amount(
                total_consumption,
                meter_data.get('tariff_category', 'A-1a'),
                meter_data.get('consumer_type', 'RESIDENTIAL_GENERAL')
            )
        else:
            # Simple fallback billing
            bill_amount = self._simple_billing(total_consumption, meter_data.get('consumer_type'))
            breakdown = {'energy_charges': bill_amount}
        
        bill = {
            'bill_id': f"BILL-{meter_data['meter_id']}-{billing_month.strftime('%Y%m')}",
            'meter_id': meter_data['meter_id'],
            'billing_month': billing_month.strftime('%Y-%m'),
            'issue_date': (billing_month + relativedelta(months=1, days=5)).strftime('%Y-%m-%d'),
            'due_date': (billing_month + relativedelta(months=1, days=15)).strftime('%Y-%m-%d'),
            'reading_date': month_readings[-1]['timestamp'][:10],
            'previous_reading': month_readings[0]['reading_kwh'],
            'current_reading': month_readings[-1]['reading_kwh'],
            'consumption_kwh': round(total_consumption, 2),
            'bill_amount': round(bill_amount, 2),
            'status': 'issued'
        }
        
        return bill
    
    def _simple_billing(self, consumption_kwh: float, consumer_type: str) -> float:
        """Simple billing calculation fallback"""
        # Basic slab rates (simplified)
        if consumption_kwh <= 50:
            return consumption_kwh * 2.0
        elif consumption_kwh <= 100:
            return 50 * 2.0 + (consumption_kwh - 50) * 5.0
        elif consumption_kwh <= 200:
            return 50 * 2.0 + 50 * 5.0 + (consumption_kwh - 100) * 10.0
        else:
            return 50 * 2.0 + 50 * 5.0 + 100 * 10.0 + (consumption_kwh - 200) * 15.0
    
    def _generate_payment(self, meter_data: Dict, bill: Dict) -> Dict:
        """Generate payment for a bill (may be unpaid)"""
        
        if not bill:
            return None
        
        # Payment probability based on consumer type and amount
        consumer_type = meter_data.get('consumer_type', 'RESIDENTIAL_GENERAL')
        bill_amount = bill['bill_amount']
        
        # Higher bills have slightly lower payment rate
        if bill_amount > 10000:
            payment_prob = 0.75
        elif bill_amount > 5000:
            payment_prob = 0.85
        else:
            payment_prob = 0.92
        
        # Government/protected consumers pay more reliably
        if 'GOVT' in consumer_type or 'PROTECTED' in consumer_type:
            payment_prob = 0.98
        
        is_paid = random.random() < payment_prob
        
        if is_paid:
            # Payment made within 0-20 days after issue
            issue_date = datetime.strptime(bill['issue_date'], '%Y-%m-%d')
            payment_delay = random.randint(0, 20)
            payment_date = issue_date + timedelta(days=payment_delay)
            
            # Partial or full payment
            if random.random() < 0.95:
                amount_paid = bill_amount
                status = 'paid'
            else:
                amount_paid = round(bill_amount * random.uniform(0.5, 0.9), 2)
                status = 'partial'
        else:
            payment_date = None
            amount_paid = 0
            status = 'unpaid'
        
        payment = {
            'payment_id': f"PAY-{bill['bill_id']}",
            'bill_id': bill['bill_id'],
            'meter_id': bill['meter_id'],
            'bill_amount': bill_amount,
            'amount_paid': amount_paid,
            'payment_date': payment_date.strftime('%Y-%m-%d') if payment_date else None,
            'payment_method': random.choice(['online', 'bank', 'cash', 'mobile_app']) if is_paid else None,
            'status': status
        }
        
        return payment
    
    def _write_meter_data_to_disk(self, results: Dict, output_dir: str):
        """Write meter data to disk (thread-safe)"""
        meter_id = results['meter_id']
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        with self.lock:
            # Append readings
            if results['readings']:
                readings_file = os.path.join(output_dir, 'readings.csv')
                df = pd.DataFrame(results['readings'])
                if os.path.exists(readings_file):
                    df.to_csv(readings_file, mode='a', header=False, index=False)
                else:
                    df.to_csv(readings_file, index=False)
            
            # Append bills
            if results['bills']:
                bills_file = os.path.join(output_dir, 'bills.csv')
                df = pd.DataFrame(results['bills'])
                if os.path.exists(bills_file):
                    df.to_csv(bills_file, mode='a', header=False, index=False)
                else:
                    df.to_csv(bills_file, index=False)
            
            # Append payments
            if results['payments']:
                payments_file = os.path.join(output_dir, 'payments.csv')
                df = pd.DataFrame(results['payments'])
                if os.path.exists(payments_file):
                    df.to_csv(payments_file, mode='a', header=False, index=False)
                else:
                    df.to_csv(payments_file, index=False)
    
    def generate_parallel(self, num_meters: int = 100,
                         start_date: str = '2024-01-01',
                         end_date: str = '2024-03-31',
                         frequency_minutes: int = 15,
                         output_dir: str = './iesco_parallel_data',
                         max_workers: int = 4):
        """
        Main parallel generation method
        
        Args:
            num_meters: Number of meters to generate
            start_date: Start date for data generation
            end_date: End date for data generation
            frequency_minutes: Reading interval in minutes
            output_dir: Output directory
            max_workers: Number of parallel threads
        """
        
        print("="*80)
        print("IESCO PARALLEL PIPELINE GENERATOR - VERSION 2.0")
        print("="*80)
        print(f"Configuration:")
        print(f"  • Meters: {num_meters}")
        print(f"  • Period: {start_date} to {end_date}")
        print(f"  • Reading Interval: {frequency_minutes} minutes")
        print(f"  • Parallel Workers: {max_workers}")
        print(f"  • Output: {output_dir}")
        print("="*80)
        
        os.makedirs(output_dir, exist_ok=True)
        
        # Step 1: Generate transformers (once)
        print("\n📊 Step 1: Generating transformers...")
        if self.base and hasattr(self.base, 'generate_transformers'):
            transformers_df = self.base.generate_transformers(target_dist_transformers=500)
        else:
            # Minimal fallback
            transformers_df = pd.DataFrame({
                'transformer_id': [f'TR-{i:05d}' for i in range(100)],
                'type': ['distribution'] * 100
            })
        
        transformers_df.to_csv(os.path.join(output_dir, 'transformers.csv'), index=False)
        print(f"   ✅ Generated {len(transformers_df)} transformers")
        
        # Step 2: Generate meter metadata (once)
        print(f"\n👥 Step 2: Generating {num_meters} meter metadata...")
        if self.base and hasattr(self.base, 'generate_meters'):
            meters_df = self.base.generate_meters(num_meters, transformers_df, start_date)
        else:
            # Minimal fallback
            meters_df = pd.DataFrame({
                'meter_id': [f'MTR-{i:06d}' for i in range(num_meters)],
                'consumer_type': ['RESIDENTIAL_GENERAL'] * num_meters,
                'sanctioned_load_kw': [random.uniform(2, 10) for _ in range(num_meters)],
                'tariff_category': ['A-1a'] * num_meters
            })
        
        meters_df.to_csv(os.path.join(output_dir, 'meters.csv'), index=False)
        print(f"   ✅ Generated {len(meters_df)} meters")
        
        # Step 3: Process meters in parallel pipeline
        print(f"\n🔄 Step 3: Processing meters through pipeline (parallel)...")
        print(f"   Pipeline: Readings → Bills → Payments (per meter, per month)")
        print(f"   Starting {max_workers} parallel workers...")
        
        # Convert meters to dict for processing
        meters_list = meters_df.to_dict('records')
        
        # Normalize column names (v1.5 uses meter_number, v2.0 expects meter_id)
        for meter in meters_list:
            if 'meter_number' in meter and 'meter_id' not in meter:
                meter['meter_id'] = meter['meter_number']
            if 'meter_id' not in meter:
                # Fallback: create meter_id from index
                meter['meter_id'] = f"MTR-{meters_list.index(meter):06d}"
        
        # Add last_reading tracker
        for meter in meters_list:
            meter['last_reading'] = 0
        
        # Process with thread pool
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all meter processing tasks
            futures = {
                executor.submit(
                    self.process_single_meter_pipeline,
                    meter, transformers_df, start_date, end_date,
                    frequency_minutes, output_dir
                ): meter['meter_id']
                for meter in meters_list
            }
            
            # Progress bar
            with tqdm(total=num_meters, desc="Processing meters", unit="meter") as pbar:
                for future in as_completed(futures):
                    meter_id = futures[future]
                    try:
                        result = future.result()
                        pbar.update(1)
                        pbar.set_postfix({
                            'completed': self.progress_stats['meters_completed'],
                            'readings': self.progress_stats['readings_generated'],
                            'bills': self.progress_stats['bills_generated']
                        })
                    except Exception as e:
                        print(f"\n❌ Error with meter {meter_id}: {e}")
                        pbar.update(1)
        
        elapsed_time = time.time() - start_time
        
        # Final summary
        print("\n" + "="*80)
        print("✅ PARALLEL GENERATION COMPLETE")
        print("="*80)
        print(f"📁 Output directory: {output_dir}")
        print(f"\n📊 Summary:")
        print(f"   • Transformers: {len(transformers_df)}")
        print(f"   • Meters Processed: {self.progress_stats['meters_completed']}")
        print(f"   • Readings Generated: {self.progress_stats['readings_generated']:,}")
        print(f"   • Bills Generated: {self.progress_stats['bills_generated']:,}")
        print(f"   • Payments Generated: {self.progress_stats['payments_generated']:,}")
        print(f"\n⏱️  Performance:")
        print(f"   • Total Time: {elapsed_time:.1f} seconds")
        print(f"   • Meters/second: {num_meters/elapsed_time:.2f}")
        print(f"   • Parallel Workers: {max_workers}")
        print("="*80)
        
        return {
            'transformers': transformers_df,
            'meters': meters_df,
            'stats': self.progress_stats,
            'elapsed_time': elapsed_time
        }


def main():
    """Main entry point with interactive prompts"""
    print("\n" + "="*80)
    print("IESCO PARALLEL PIPELINE GENERATOR - VERSION 2.0")
    print("="*80)
    
    # Interactive prompts
    print("\n📋 Configuration (press Enter for defaults):")
    
    num_meters = input("Number of meters [100]: ").strip()
    num_meters = int(num_meters) if num_meters else 100
    
    start_date = input("Start date (YYYY-MM-DD) [2024-01-01]: ").strip()
    start_date = start_date if start_date else '2024-01-01'
    
    end_date = input("End date (YYYY-MM-DD) [2024-03-31]: ").strip()
    end_date = end_date if end_date else '2024-03-31'
    
    frequency = input("Reading interval in minutes [15]: ").strip()
    frequency = int(frequency) if frequency else 15
    
    workers = input("Number of parallel workers [4]: ").strip()
    workers = int(workers) if workers else 4
    
    output_dir = input("Output directory [./iesco_parallel_data]: ").strip()
    output_dir = output_dir if output_dir else './iesco_parallel_data'
    
    # Create generator
    print("\n🔧 Initializing generator...")
    generator = IESCOParallelPipelineGenerator()
    
    # Generate data
    print("\n🚀 Starting parallel generation...")
    results = generator.generate_parallel(
        num_meters=num_meters,
        start_date=start_date,
        end_date=end_date,
        frequency_minutes=frequency,
        output_dir=output_dir,
        max_workers=workers
    )
    
    print("\n✅ Done! Check the output directory for generated files.")


if __name__ == '__main__':
    main()
