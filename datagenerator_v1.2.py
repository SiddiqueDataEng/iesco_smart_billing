"""
Enhanced IESCO Smart Meter Data Generator with Temporal Evolution
Simulates dynamic grid with new connections, meter replacements, and infrastructure changes
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from faker import Faker
import os
import json
from tqdm import tqdm
import argparse
from typing import Tuple, Dict, List, Optional
from collections import defaultdict

# Initialize Faker
fake = Faker('en_PK')
Faker.seed(42)
np.random.seed(42)
random.seed(42)

class IESCODynamicDataGenerator:
    def __init__(self):
        # Same tariff categories as before
        self.tariff_categories = {
            'A-1a': {'name': 'Residential (Single Phase)', 'min_load': 1, 'max_load': 5, 'category': 'residential'},
            'A-1b': {'name': 'Residential (Three Phase)', 'min_load': 5, 'max_load': 30, 'category': 'residential'},
            'A-2a': {'name': 'Commercial (Single Phase)', 'min_load': 1, 'max_load': 5, 'category': 'commercial'},
            'A-2b': {'name': 'Commercial (Three Phase)', 'min_load': 5, 'max_load': 50, 'category': 'commercial'},
            'B-1': {'name': 'Industrial (Small)', 'min_load': 5, 'max_load': 25, 'category': 'industrial'},
            'B-2': {'name': 'Industrial (Large)', 'min_load': 25, 'max_load': 500, 'category': 'industrial'},
            'C-1': {'name': 'Agricultural', 'min_load': 5, 'max_load': 50, 'category': 'agricultural'},
            'D-1': {'name': 'Bulk Supply', 'min_load': 50, 'max_load': 1000, 'category': 'bulk'}
        }
        
        # Zones configuration (same as before)
        self.zones = {
            'NORTH_ZONE': {
                'divisions': ['SATELLITE TOWN', 'WESTRIDGE', 'CHAKLALA'],
                'sub_divisions': {
                    'SATELLITE TOWN': ['DHOKE KALA KHAN', 'MORGHAH', 'SATELLITE TOWN MAIN'],
                    'WESTRIDGE': ['WESTRIDGE MAIN', 'NAWAB TOWN', 'SHAMSABAD'],
                    'CHAKLALA': ['CHAKLALA CANTT', 'DEFENCE', 'BAHRIA PHASE-7']
                },
                'grid_stations': ['132kV SATELLITE TOWN', '132kV WESTRIDGE', '220kV CHAKLALA'],
                'location': {'lat_range': (33.55, 33.65), 'lon_range': (73.00, 73.10)},
                'growth_rate': 0.15  # 15% new connections per year
            },
            'CENTRAL_ZONE': {
                'divisions': ['SADDAR', 'RAWALPINDI CANTT', 'LIAQAT BAGH'],
                'sub_divisions': {
                    'SADDAR': ['SADDAR MAIN', 'RAJA BAZAAR', 'KARIAL'],
                    'RAWALPINDI CANTT': ['CANTT MAIN', 'SATTELITE', 'DHOKE RATTA'],
                    'LIAQAT BAGH': ['LIAQAT BAGH MAIN', 'SHAH KHALID', 'WARIS KHAN']
                },
                'grid_stations': ['132kV SADDAR', '132kV CANTT', '220kV GULBERG'],
                'location': {'lat_range': (33.60, 33.70), 'lon_range': (73.05, 73.15)},
                'growth_rate': 0.12  # 12% new connections per year
            },
            'SOUTH_ZONE': {
                'divisions': ['GULBERG', 'KOHAT ROAD', 'NEW CITY'],
                'sub_divisions': {
                    'GULBERG': ['GULBERG MAIN', 'CHANDNI CHOWK', 'COMMERCIAL MARKET'],
                    'KOHAT ROAD': ['KOHAT ROAD MAIN', 'DEFENCE PHASE-1', 'GHAZI TOWN'],
                    'NEW CITY': ['NEW CITY MAIN', 'DHOKE NAJU', 'SHAHBAZ TOWN']
                },
                'grid_stations': ['132kV GULBERG', '132kV KOHAT', '220kV NEW CITY'],
                'location': {'lat_range': (33.50, 33.60), 'lon_range': (73.00, 73.10)},
                'growth_rate': 0.10  # 10% new connections per year
            }
        }
        
        # Transformer specs with upgrade paths
        self.transformer_specs = {
            'grid_transformer': {
                'types': [
                    {'rating_mva': 250, 'voltage': '220/132kV', 'count': 5, 'upgrade_to': 400},
                    {'rating_mva': 160, 'voltage': '132/33kV', 'count': 15, 'upgrade_to': 250},
                    {'rating_mva': 100, 'voltage': '132/11kV', 'count': 25, 'upgrade_to': 160}
                ]
            },
            'distribution_transformer': {
                'types': [
                    {'rating_kva': 1500, 'voltage': '11/0.4kV', 'phase': '3-phase', 'count': 50, 'upgrade_to': 2000},
                    {'rating_kva': 1000, 'voltage': '11/0.4kV', 'phase': '3-phase', 'count': 100, 'upgrade_to': 1500},
                    {'rating_kva': 750, 'voltage': '11/0.4kV', 'phase': '3-phase', 'count': 150, 'upgrade_to': 1000},
                    {'rating_kva': 500, 'voltage': '11/0.4kV', 'phase': '3-phase', 'count': 200, 'upgrade_to': 750},
                    {'rating_kva': 250, 'voltage': '11/0.4kV', 'phase': '1-phase', 'count': 300, 'upgrade_to': 400},
                    {'rating_kva': 100, 'voltage': '11/0.4kV', 'phase': '1-phase', 'count': 500, 'upgrade_to': 200}
                ]
            }
        }
        
        # Event probabilities (per month)
        self.event_probabilities = {
            'new_connection': 0.015,      # 1.5% increase in meters per month
            'meter_replacement': 0.005,     # 0.5% meters replaced per month
            'meter_failure': 0.003,         # 0.3% meters fail per month
            'transformer_upgrade': 0.001,    # 0.1% transformers upgraded per month
            'consumer_churn': 0.002,         # 0.2% consumers disconnect per month
            'tariff_change': 0.001           # 0.1% consumers change tariff
        }
        
        # Data quality issues
        self.issue_probabilities = {
            'missing_reading': 0.02,
            'negative_reading': 0.005,
            'zero_reading': 0.01,
            'abnormal_spike': 0.01,
            'voltage_sag': 0.015,
            'frequency_variation': 0.01,
            'signal_drop': 0.02,
            'battery_fault': 0.005,
            'meter_tamper': 0.003,
            'reverse_energy': 0.002
        }
        
        # Track events for history
        self.events_log = []

    def generate_initial_transformers(self, target_dist_transformers: int = 1500) -> pd.DataFrame:
        """Generate initial transformer infrastructure"""
        transformers = []
        transformer_id = 1
        
        # Generate Grid Transformers
        for zone_name, zone_info in self.zones.items():
            for grid_station in zone_info['grid_stations']:
                num_grid_trans = random.randint(2, 4)
                for i in range(num_grid_trans):
                    trans_type = random.choice(self.transformer_specs['grid_transformer']['types'])
                    install_date = fake.date_between(start_date='-20y', end_date='-5y')
                    
                    trans = {
                        'transformer_id': f'GT{transformer_id:06d}',
                        'transformer_type': 'grid',
                        'zone': zone_name,
                        'grid_station': grid_station,
                        'rating_mva': trans_type['rating_mva'],
                        'initial_rating_mva': trans_type['rating_mva'],
                        'voltage_level': trans_type['voltage'],
                        'manufacturer': random.choice(['Siemens', 'ABB', 'Heavy Electrical Complex', 'Toshiba']),
                        'installation_date': install_date,
                        'last_maintenance': fake.date_between(start_date='-1y', end_date='-1d'),
                        'upgrade_date': None,
                        'upgrade_history': [],
                        'oil_level_status': 'Normal',
                        'temperature_c': random.gauss(65, 10),
                        'load_factor': random.uniform(0.4, 0.95),
                        'latitude': random.uniform(*zone_info['location']['lat_range']),
                        'longitude': random.uniform(*zone_info['location']['lon_range']),
                        'status': 'Active',
                        'capacity_utilization_pct': random.uniform(40, 90)
                    }
                    transformers.append(trans)
                    transformer_id += 1
        
        # Generate Distribution Transformers
        grid_transformers = [t for t in transformers if t['transformer_type'] == 'grid']
        dist_per_grid = target_dist_transformers // len(grid_transformers)
        
        for grid_trans in grid_transformers:
            num_dist = random.randint(int(dist_per_grid * 0.8), int(dist_per_grid * 1.2))
            
            for j in range(num_dist):
                trans_type = random.choices(
                    self.transformer_specs['distribution_transformer']['types'],
                    weights=[t['count'] for t in self.transformer_specs['distribution_transformer']['types']],
                    k=1
                )[0]
                
                zone_info = self.zones[grid_trans['zone']]
                division = random.choice(zone_info['divisions'])
                sub_division = random.choice(zone_info['sub_divisions'][division])
                feeder_name = f"FD{random.randint(1000, 9999)} {division[:3]}_{sub_division[:3]}"
                
                install_date = fake.date_between(start_date='-15y', end_date='-1y')
                
                trans = {
                    'transformer_id': f'DT{transformer_id:06d}',
                    'transformer_type': 'distribution',
                    'zone': grid_trans['zone'],
                    'division': division,
                    'sub_division': sub_division,
                    'feeder_name': feeder_name,
                    'grid_transformer_id': grid_trans['transformer_id'],
                    'rating_kva': trans_type['rating_kva'],
                    'initial_rating_kva': trans_type['rating_kva'],
                    'voltage_level': trans_type['voltage'],
                    'phase_type': trans_type['phase'],
                    'manufacturer': random.choice(['Pakistan Transformers', 'Siemens', 'ABB', 'Local']),
                    'installation_date': install_date,
                    'last_maintenance': fake.date_between(start_date='-1y', end_date='-1d'),
                    'upgrade_date': None,
                    'upgrade_history': [],
                    'oil_level_status': random.choice(['Normal', 'Normal', 'Low', 'Critical']),
                    'max_load_kva': trans_type['rating_kva'] * 0.8,
                    'current_load_kva': random.uniform(100, trans_type['rating_kva'] * 0.7),
                    'tap_position': random.randint(1, 5),
                    'latitude': grid_trans['latitude'] + random.uniform(-0.02, 0.02),
                    'longitude': grid_trans['longitude'] + random.uniform(-0.02, 0.02),
                    'status': 'Active',
                    'peak_load_season': random.choice(['Summer', 'Winter', 'Both']),
                    'capacity_utilization_pct': random.uniform(30, 85),
                    'consumers_connected': 0  # Will be updated
                }
                transformers.append(trans)
                transformer_id += 1
        
        return pd.DataFrame(transformers)

    def generate_initial_meters(self, 
                               num_meters: int,
                               transformers_df: pd.DataFrame,
                               current_date: str) -> pd.DataFrame:
        """Generate initial set of meters"""
        
        meters = []
        distribution_transformers = transformers_df[transformers_df['transformer_type'] == 'distribution']
        
        # Distribute meters across transformers
        transformer_consumer_counts = {}
        
        for idx, transformer in distribution_transformers.iterrows():
            # Calculate capacity-based consumer allocation
            capacity = transformer['rating_kva']
            # Rough estimate: 1 kVA = ~0.5 consumers (average 2kW each)
            max_consumers = int(capacity / 2)
            
            # Initial loading (30-70% of capacity)
            num_trans_consumers = random.randint(int(max_consumers * 0.3), int(max_consumers * 0.7))
            transformer_consumer_counts[transformer['transformer_id']] = num_trans_consumers
        
        # Scale to meet total num_meters
        total_capacity_consumers = sum(transformer_consumer_counts.values())
        scale_factor = num_meters / total_capacity_consumers if total_capacity_consumers > 0 else 1
        
        # Generate meters
        meter_id = 1
        for idx, transformer in distribution_transformers.iterrows():
            transformer_id = transformer['transformer_id']
            target_consumers = int(transformer_consumer_counts[transformer_id] * scale_factor)
            
            for _ in range(target_consumers):
                if len(meters) >= num_meters:
                    break
                
                # Determine tariff based on phase
                if transformer['phase_type'] == '1-phase':
                    tariff_options = ['A-1a', 'A-2a']
                    weights = [0.8, 0.2]
                else:
                    tariff_options = ['A-1b', 'A-2b', 'B-1', 'B-2', 'C-1', 'D-1']
                    weights = [0.4, 0.2, 0.15, 0.1, 0.1, 0.05]
                
                tariff = random.choices(tariff_options, weights=weights, k=1)[0]
                
                # Connection date (some old, some recent)
                connection_date = fake.date_between(
                    start_date=pd.to_datetime(current_date) - timedelta(days=5*365),
                    end_date=current_date
                )
                
                consumer_id = f"CI{random.randint(1000000, 9999999)}"
                meter_number = f"{random.randint(10000000000, 99999999999)}"
                
                # Generate meter lifecycle info
                meter = {
                    'consumer_id': consumer_id,
                    'meter_number': meter_number,
                    'previous_meter_number': None,  # For replacements
                    'meter_generation': 1,  # First meter
                    'installation_date': connection_date,
                    'connection_date': connection_date,
                    'deactivation_date': None,
                    'is_active': True,
                    'reference_no': f"11 {random.randint(10000, 99999)} {random.randint(1000000, 9999999)} U",
                    'name': fake.name(),
                    'father_name': fake.name_male() if random.random() > 0.3 else fake.name_female(),
                    'cnic': f"{random.randint(10000, 99999)}-{random.randint(1000000, 9999999)}-{random.randint(1, 9)}",
                    'phone': f"03{random.randint(0, 9)}-{random.randint(1000000, 9999999)}",
                    'address': self._generate_address(transformer['division'], transformer['sub_division']),
                    'tariff_category': tariff,
                    'tariff_description': self.tariff_categories[tariff]['name'],
                    'consumer_category': self.tariff_categories[tariff]['category'],
                    'original_tariff': tariff,
                    'tariff_change_history': [],
                    'connected_load_kw': round(random.uniform(
                        self.tariff_categories[tariff]['min_load'],
                        self.tariff_categories[tariff]['max_load']
                    ), 2),
                    'sanctioned_load_kw': 0,  # Will calculate
                    'zone': transformer['zone'],
                    'division': transformer['division'],
                    'sub_division': transformer['sub_division'],
                    'feeder_name': transformer['feeder_name'],
                    'grid_transformer_id': transformer['grid_transformer_id'],
                    'distribution_transformer_id': transformer_id,
                    'phase_type': transformer['phase_type'],
                    'meter_type': random.choices(['Smart', 'Smart', 'Conventional'], weights=[0.7, 0.7, 0.3])[0],
                    'meter_make': random.choice(['Landis+Gyr', 'Siemens', 'Itron', 'Elster']),
                    'meter_model': random.choice(['EM1200', 'SGM3000', 'AX-03', 'PM-500']),
                    'latitude': transformer['latitude'] + random.uniform(-0.005, 0.005),
                    'longitude': transformer['longitude'] + random.uniform(-0.005, 0.005),
                    'status': 'Active',
                    'has_solar': random.random() > 0.85,
                    'solar_capacity_kw': round(random.uniform(1, 10), 2) if random.random() > 0.85 else 0,
                    'solar_installation_date': fake.date_between(start_date='-3y', end_date=current_date) if random.random() > 0.85 else None,
                    'average_monthly_consumption': 0,
                    'billing_status': 'Regular',
                    'payment_method': random.choice(['Bank', 'JazzCash', 'EasyPaisa', 'Online']),
                    'email': fake.email(),
                    'lifecycle_events': []
                }
                
                # Calculate sanctioned load (slightly higher than connected)
                meter['sanctioned_load_kw'] = meter['connected_load_kw'] * random.uniform(1.1, 1.3)
                
                meters.append(meter)
                meter_id += 1
            
            if len(meters) >= num_meters:
                break
        
        return pd.DataFrame(meters[:num_meters])

    def _generate_address(self, division, sub_division):
        """Generate realistic Pakistani address"""
        street_names = ['Main Bazaar', 'Commercial Area', 'Gulshan-e-Iqbal', 'Defence Road',
                       'University Road', 'Murree Road', 'Kashmir Road', 'College Road',
                       'Hospital Road', 'Railway Road', 'Airport Road']
        
        return f"H.No. {random.randint(1, 500)}, St. {random.randint(1, 20)}, {random.choice(street_names)}, {sub_division}, RAWALPINDI"

    def simulate_monthly_events(self,
                               meters_df: pd.DataFrame,
                               transformers_df: pd.DataFrame,
                               current_date: str,
                               month_index: int) -> Tuple[pd.DataFrame, pd.DataFrame, List]:
        """
        Simulate monthly events: new connections, meter replacements, upgrades
        Returns updated meters, transformers, and events log
        """
        
        current_date = pd.to_datetime(current_date)
        events = []
        
        # Make copies to avoid modifying original
        meters = meters_df.copy() if isinstance(meters_df, pd.DataFrame) else meters_df
        transformers = transformers_df.copy() if isinstance(transformers_df, pd.DataFrame) else transformers_df
        
        # Convert to list for easier manipulation if needed
        if isinstance(meters, pd.DataFrame):
            meters_list = meters.to_dict('records')
        else:
            meters_list = meters
        
        if isinstance(transformers, pd.DataFrame):
            transformers_list = transformers.to_dict('records')
        else:
            transformers_list = transformers
        
        # 1. NEW CONNECTIONS
        # Calculate new connections based on zone growth rates
        for zone_name, zone_info in self.zones.items():
            # Monthly new connections = (annual growth rate / 12) * current meters in zone
            zone_meters = [m for m in meters_list if m['zone'] == zone_name and m['is_active']]
            annual_growth = zone_info['growth_rate']
            monthly_growth = annual_growth / 12
            
            # New connections this month (Poisson distribution)
            new_connections = np.random.poisson(max(1, int(len(zone_meters) * monthly_growth)))
            
            for _ in range(new_connections):
                # Find a transformer in this zone with capacity
                zone_transformers = [t for t in transformers_list 
                                   if t['zone'] == zone_name and t['transformer_type'] == 'distribution']
                
                if not zone_transformers:
                    continue
                
                # Prefer transformers with lower utilization
                transformer = min(zone_transformers, key=lambda x: x['capacity_utilization_pct'])
                
                # Generate new meter
                new_meter = self._generate_new_meter(transformer, current_date, meters_list)
                meters_list.append(new_meter)
                
                # Update transformer consumer count
                transformer['consumers_connected'] = transformer.get('consumers_connected', 0) + 1
                transformer['capacity_utilization_pct'] = min(95, 
                    transformer['capacity_utilization_pct'] + random.uniform(0.5, 2))
                
                events.append({
                    'date': current_date,
                    'event_type': 'new_connection',
                    'meter_number': new_meter['meter_number'],
                    'consumer_id': new_meter['consumer_id'],
                    'transformer_id': transformer['transformer_id'],
                    'details': f"New {new_meter['consumer_category']} connection"
                })
        
        # 2. METER REPLACEMENTS (failed meters)
        active_meters = [m for m in meters_list if m['is_active']]
        replacements = np.random.poisson(int(len(active_meters) * self.event_probabilities['meter_replacement']))
        
        for _ in range(min(replacements, len(active_meters))):
            # Select random meter for replacement (weighted by age)
            meter_to_replace = random.choice(active_meters)
            
            # Create replacement meter
            replacement = self._replace_meter(meter_to_replace, current_date)
            meters_list.append(replacement)
            
            # Deactivate old meter
            meter_to_replace['is_active'] = False
            meter_to_replace['deactivation_date'] = current_date
            meter_to_replace['status'] = 'Replaced'
            
            events.append({
                'date': current_date,
                'event_type': 'meter_replacement',
                'old_meter': meter_to_replace['meter_number'],
                'new_meter': replacement['meter_number'],
                'consumer_id': meter_to_replace['consumer_id'],
                'reason': random.choice(['Failed', 'Upgraded', 'Damaged', 'Stolen'])
            })
        
        # 3. METER FAILURES (temporary outages)
        failures = np.random.poisson(int(len(active_meters) * self.event_probabilities['meter_failure']))
        
        for _ in range(min(failures, len(active_meters))):
            meter = random.choice(active_meters)
            # Mark meter as failed (will affect readings)
            failure_duration = random.randint(1, 7)  # 1-7 days
            recovery_date = current_date + timedelta(days=failure_duration)
            
            events.append({
                'date': current_date,
                'event_type': 'meter_failure',
                'meter_number': meter['meter_number'],
                'consumer_id': meter['consumer_id'],
                'failure_duration_days': failure_duration,
                'recovery_date': recovery_date,
                'details': random.choice(['Communication loss', 'Hardware fault', 'Battery dead'])
            })
        
        # 4. TRANSFORMER UPGRADES
        overloaded_transformers = [t for t in transformers_list 
                                 if t['capacity_utilization_pct'] > 85 and t['transformer_type'] == 'distribution']
        
        upgrades = np.random.poisson(int(len(overloaded_transformers) * self.event_probabilities['transformer_upgrade']))
        
        for transformer in random.sample(overloaded_transformers, min(upgrades, len(overloaded_transformers))):
            old_rating = transformer['rating_kva']
            
            # Find upgrade path
            for spec in self.transformer_specs['distribution_transformer']['types']:
                if spec['rating_kva'] == old_rating:
                    new_rating = spec['upgrade_to']
                    break
            else:
                new_rating = old_rating * 1.5  # Default 50% upgrade
            
            # Record upgrade
            transformer['rating_kva'] = new_rating
            transformer['upgrade_date'] = current_date
            transformer['upgrade_history'] = transformer.get('upgrade_history', []) + [{
                'date': current_date,
                'old_rating': old_rating,
                'new_rating': new_rating
            }]
            transformer['capacity_utilization_pct'] = transformer['capacity_utilization_pct'] * (old_rating / new_rating)
            
            events.append({
                'date': current_date,
                'event_type': 'transformer_upgrade',
                'transformer_id': transformer['transformer_id'],
                'old_rating_kva': old_rating,
                'new_rating_kva': new_rating,
                'reason': 'Capacity enhancement'
            })
        
        # 5. CONSUMER CHURN (disconnections)
        churns = np.random.poisson(int(len(active_meters) * self.event_probabilities['consumer_churn']))
        
        for _ in range(min(churns, len(active_meters))):
            meter = random.choice(active_meters)
            
            # Disconnect consumer
            meter['is_active'] = False
            meter['deactivation_date'] = current_date
            meter['status'] = random.choice(['Disconnected', 'Suspended', 'Closed'])
            
            # Update transformer load
            for t in transformers_list:
                if t['transformer_id'] == meter['distribution_transformer_id']:
                    t['consumers_connected'] = max(0, t.get('consumers_connected', 1) - 1)
                    t['capacity_utilization_pct'] = max(10, t['capacity_utilization_pct'] - random.uniform(1, 3))
                    break
            
            events.append({
                'date': current_date,
                'event_type': 'consumer_churn',
                'meter_number': meter['meter_number'],
                'consumer_id': meter['consumer_id'],
                'reason': random.choice(['Non-payment', 'Relocated', 'Deceased', 'Business closed'])
            })
        
        # 6. TARIFF CHANGES
        tariff_changes = np.random.poisson(int(len(active_meters) * self.event_probabilities['tariff_change']))
        
        for _ in range(min(tariff_changes, len(active_meters))):
            meter = random.choice(active_meters)
            old_tariff = meter['tariff_category']
            
            # Possible new tariffs based on category
            if meter['consumer_category'] == 'residential':
                new_tariff = random.choice(['A-1a', 'A-1b'])
            elif meter['consumer_category'] == 'commercial':
                new_tariff = random.choice(['A-2a', 'A-2b'])
            elif meter['consumer_category'] == 'industrial':
                new_tariff = random.choice(['B-1', 'B-2'])
            else:
                continue
            
            if new_tariff != old_tariff:
                meter['tariff_category'] = new_tariff
                meter['tariff_change_history'] = meter.get('tariff_change_history', []) + [{
                    'date': current_date,
                    'old_tariff': old_tariff,
                    'new_tariff': new_tariff
                }]
                
                events.append({
                    'date': current_date,
                    'event_type': 'tariff_change',
                    'meter_number': meter['meter_number'],
                    'consumer_id': meter['consumer_id'],
                    'old_tariff': old_tariff,
                    'new_tariff': new_tariff
                })
        
        # Convert back to DataFrames
        meters_df = pd.DataFrame(meters_list)
        transformers_df = pd.DataFrame(transformers_list)
        
        return meters_df, transformers_df, events

    def _generate_new_meter(self, transformer, connection_date, existing_meters):
        """Generate a new meter connection"""
        
        # Determine tariff based on phase
        if transformer['phase_type'] == '1-phase':
            tariff_options = ['A-1a', 'A-2a']
            weights = [0.85, 0.15]  # More residential in new connections
        else:
            tariff_options = ['A-1b', 'A-2b', 'B-1', 'C-1']
            weights = [0.5, 0.3, 0.15, 0.05]
        
        tariff = random.choices(tariff_options, weights=weights, k=1)[0]
        
        # Generate unique IDs
        consumer_id = f"CI{random.randint(1000000, 9999999)}"
        # Ensure unique meter number
        while True:
            meter_number = f"{random.randint(10000000000, 99999999999)}"
            if not any(m['meter_number'] == meter_number for m in existing_meters):
                break
        
        return {
            'consumer_id': consumer_id,
            'meter_number': meter_number,
            'previous_meter_number': None,
            'meter_generation': 1,
            'installation_date': connection_date,
            'connection_date': connection_date,
            'deactivation_date': None,
            'is_active': True,
            'reference_no': f"11 {random.randint(10000, 99999)} {random.randint(1000000, 9999999)} U",
            'name': fake.name(),
            'father_name': fake.name_male() if random.random() > 0.3 else fake.name_female(),
            'cnic': f"{random.randint(10000, 99999)}-{random.randint(1000000, 9999999)}-{random.randint(1, 9)}",
            'phone': f"03{random.randint(0, 9)}-{random.randint(1000000, 9999999)}",
            'address': self._generate_address(transformer['division'], transformer['sub_division']),
            'tariff_category': tariff,
            'tariff_description': self.tariff_categories[tariff]['name'],
            'consumer_category': self.tariff_categories[tariff]['category'],
            'original_tariff': tariff,
            'tariff_change_history': [],
            'connected_load_kw': round(random.uniform(
                self.tariff_categories[tariff]['min_load'],
                self.tariff_categories[tariff]['max_load']
            ), 2),
            'sanctioned_load_kw': 0,
            'zone': transformer['zone'],
            'division': transformer['division'],
            'sub_division': transformer['sub_division'],
            'feeder_name': transformer['feeder_name'],
            'grid_transformer_id': transformer['grid_transformer_id'],
            'distribution_transformer_id': transformer['transformer_id'],
            'phase_type': transformer['phase_type'],
            'meter_type': 'Smart',  # New meters are smart
            'meter_make': random.choice(['Landis+Gyr', 'Siemens', 'Itron']),
            'meter_model': random.choice(['EM1200', 'SGM3000', 'AX-03']),
            'latitude': transformer['latitude'] + random.uniform(-0.005, 0.005),
            'longitude': transformer['longitude'] + random.uniform(-0.005, 0.005),
            'status': 'Active',
            'has_solar': random.random() > 0.9,  # New connections less likely to have solar
            'solar_capacity_kw': round(random.uniform(1, 5), 2) if random.random() > 0.9 else 0,
            'solar_installation_date': connection_date if random.random() > 0.9 else None,
            'average_monthly_consumption': 0,
            'billing_status': 'Regular',
            'payment_method': random.choice(['JazzCash', 'EasyPaisa', 'Online']),  # New prefer digital
            'email': fake.email(),
            'lifecycle_events': []
        }

    def _replace_meter(self, old_meter, replacement_date):
        """Create a replacement meter for a failed/upgraded one"""
        
        new_meter = old_meter.copy()
        
        # Generate new meter number
        new_meter['meter_number'] = f"{random.randint(10000000000, 99999999999)}"
        new_meter['previous_meter_number'] = old_meter['meter_number']
        new_meter['meter_generation'] = old_meter.get('meter_generation', 1) + 1
        new_meter['installation_date'] = replacement_date
        new_meter['deactivation_date'] = None
        new_meter['is_active'] = True
        new_meter['status'] = 'Active'
        
        # Update meter type (usually upgrade to smart)
        if old_meter['meter_type'] != 'Smart' and random.random() > 0.3:
            new_meter['meter_type'] = 'Smart'
            new_meter['meter_make'] = random.choice(['Landis+Gyr', 'Siemens', 'Itron'])
            new_meter['meter_model'] = random.choice(['EM1200', 'SGM3000'])
        
        # Add to lifecycle events
        if 'lifecycle_events' not in new_meter:
            new_meter['lifecycle_events'] = []
        new_meter['lifecycle_events'].append({
            'date': replacement_date,
            'event': 'meter_replacement',
            'old_meter': old_meter['meter_number']
        })
        
        return new_meter

    def generate_readings_dynamic(self,
                                 meters_df: pd.DataFrame,
                                 transformers_df: pd.DataFrame,
                                 events_log: List,
                                 start_date: str,
                                 end_date: str,
                                 frequency_minutes: int = 15) -> pd.DataFrame:
        """
        Generate readings considering meter failures, replacements, and new connections
        """
        
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        
        # Generate all timestamps
        all_timestamps = pd.date_range(start=start, end=end, freq=f'{frequency_minutes}min')
        
        readings = []
        
        # Create failure periods lookup
        failures_by_meter = defaultdict(list)
        for event in events_log:
            if event['event_type'] == 'meter_failure':
                failure_start = event['date']
                failure_end = event.get('recovery_date', failure_start + timedelta(days=event.get('failure_duration_days', 1)))
                failures_by_meter[event['meter_number']].append((failure_start, failure_end))
        
        # Group meters by transformer for batch processing
        meters_by_transformer = meters_df.groupby('distribution_transformer_id')
        
        for transformer_id, meter_group in tqdm(meters_by_transformer, desc="Processing transformers"):
            # Get transformer info
            transformer = transformers_df[transformers_df['transformer_id'] == transformer_id].iloc[0]
            
            for _, meter in meter_group.iterrows():
                meter_number = meter['meter_number']
                consumer_id = meter['consumer_id']
                tariff = meter['tariff_category']
                installation_date = pd.to_datetime(meter['installation_date'])
                deactivation_date = pd.to_datetime(meter['deactivation_date']) if pd.notna(meter['deactivation_date']) else None
                
                # Get failures for this meter
                meter_failures = failures_by_meter.get(meter_number, [])
                
                # Consumption parameters based on tariff
                base_min, base_max, peak_min, peak_max = self._get_consumption_params(tariff)
                
                cumulative_reading = 0
                previous_reading = 0
                
                for timestamp in all_timestamps:
                    # Check if meter was active at this timestamp
                    if timestamp < installation_date:
                        continue
                    if deactivation_date and timestamp > deactivation_date:
                        continue
                    
                    # Check if meter was in failure period
                    in_failure = any(start <= timestamp <= end for start, end in meter_failures)
                    if in_failure:
                        continue  # Skip readings during failure
                    
                    hour = timestamp.hour
                    month = timestamp.month
                    day = timestamp.dayofweek
                    
                    # Seasonal and daily patterns
                    is_peak = (6 <= hour <= 10) or (18 <= hour <= 23)
                    weekend_multiplier = 1.2 if day >= 5 else 1.0
                    summer_months = [5, 6, 7, 8, 9]
                    seasonal_multiplier = 1.4 if month in summer_months else 1.0
                    
                    # Check if meter had solar during this time
                    has_solar = meter['has_solar']
                    solar_install_date = pd.to_datetime(meter['solar_installation_date']) if pd.notna(meter.get('solar_installation_date')) else None
                    solar_active = has_solar and (solar_install_date is None or timestamp >= solar_install_date)
                    
                    # Base consumption
                    if is_peak:
                        base_consumption = random.uniform(peak_min, peak_max)
                    else:
                        base_consumption = random.uniform(base_min, base_max)
                    
                    # Apply multipliers
                    consumption = base_consumption * weekend_multiplier * seasonal_multiplier
                    
                    # Solar impact (reduce consumption during daylight)
                    if solar_active and 8 <= hour <= 17:
                        solar_factor = random.uniform(0.3, 0.8)
                        consumption *= solar_factor
                    
                    # Add random variation
                    consumption *= random.uniform(0.9, 1.1)
                    
                    cumulative_reading += consumption
                    
                    # Generate electrical parameters
                    voltage = self._generate_voltage(timestamp, transformer['capacity_utilization_pct'] / 100)
                    current = (consumption * 1000) / voltage if voltage > 0 else 0
                    frequency = self._generate_frequency(timestamp)
                    power_factor = self._generate_power_factor(consumption, tariff)
                    temperature = self._generate_temperature(timestamp)
                    signal_strength = self._generate_signal_strength(timestamp)
                    battery_voltage = self._generate_battery_voltage()
                    
                    # Apply data quality issues
                    rand_val = random.random()
                    consumption, cumulative_reading, voltage, frequency, signal_strength, battery_voltage, quality_flag = \
                        self._apply_data_quality_issues(rand_val, consumption, cumulative_reading,
                                                       previous_reading, voltage, frequency,
                                                       signal_strength, battery_voltage)
                    
                    reading = {
                        'timestamp': timestamp,
                        'meter_number': meter_number,
                        'consumer_id': consumer_id,
                        'distribution_transformer_id': transformer_id,
                        'grid_transformer_id': meter['grid_transformer_id'],
                        'reading_kwh': round(cumulative_reading, 3),
                        'energy_consumed_kwh': round(consumption, 3),
                        'voltage_v': round(voltage, 1),
                        'current_a': round(current, 2),
                        'frequency_hz': round(frequency, 2),
                        'power_factor': round(power_factor, 3),
                        'temperature_c': round(temperature, 1),
                        'signal_strength_dbm': round(signal_strength, 1),
                        'battery_voltage_v': round(battery_voltage, 2),
                        'data_quality_flag': quality_flag,
                        'meter_generation': meter['meter_generation'],
                        'solar_active': solar_active if 'solar_active' in locals() else False,
                        'is_peak_hour': is_peak
                    }
                    
                    readings.append(reading)
                    previous_reading = cumulative_reading
        
        return pd.DataFrame(readings)

    def _get_consumption_params(self, tariff):
        """Get consumption parameters based on tariff"""
        if 'A-1' in tariff:
            return 0.1, 0.5, 0.5, 0.8
        elif 'A-2' in tariff:
            return 0.3, 1.0, 1.0, 1.8
        elif 'B-1' in tariff:
            return 1.0, 3.0, 3.0, 5.0
        elif 'B-2' in tariff:
            return 3.0, 8.0, 8.0, 15.0
        elif 'C-1' in tariff:
            return 0.5, 2.0, 2.0, 4.0
        else:
            return 5.0, 15.0, 15.0, 25.0

    def _generate_voltage(self, timestamp, load_factor):
        """Generate voltage with load correlation"""
        hour = timestamp.hour
        
        if 10 <= hour <= 14:
            base = 225
        elif 18 <= hour <= 22:
            base = 220
        else:
            base = 230
        
        voltage = base * (1 - 0.03 * load_factor)
        return voltage + random.gauss(0, 2)

    def _generate_frequency(self, timestamp):
        """Generate frequency"""
        hour = timestamp.hour
        if 18 <= hour <= 22:
            return 49.8 + random.gauss(0, 0.1)
        elif 1 <= hour <= 4:
            return 50.1 + random.gauss(0, 0.1)
        else:
            return 50.0 + random.gauss(0, 0.1)

    def _generate_power_factor(self, consumption, tariff):
        """Generate power factor"""
        if 'B' in tariff:
            base = 0.95
        elif 'A-2' in tariff:
            base = 0.92
        else:
            base = 0.88
        
        return max(0.8, min(0.99, base + random.gauss(0, 0.02)))

    def _generate_temperature(self, timestamp):
        """Generate temperature"""
        hour = timestamp.hour
        month = timestamp.month
        
        daily_temp = 32 if 14 <= hour <= 16 else (20 if 4 <= hour <= 6 else 26)
        
        if month in [5, 6, 7, 8]:
            seasonal = 35
        elif month in [12, 1, 2]:
            seasonal = 10
        else:
            seasonal = 25
        
        return (daily_temp + seasonal) / 2 + random.gauss(0, 3)

    def _generate_signal_strength(self, timestamp):
        """Generate signal strength"""
        hour = timestamp.hour
        if 0 <= hour <= 5:
            return -65 + random.gauss(0, 5)
        elif 9 <= hour <= 12 or 18 <= hour <= 21:
            return -75 + random.gauss(0, 5)
        else:
            return -70 + random.gauss(0, 5)

    def _generate_battery_voltage(self):
        """Generate battery voltage"""
        return 3.7 + random.gauss(0, 0.1)

    def _apply_data_quality_issues(self, rand_val, consumption, cumulative_reading,
                                  previous_reading, voltage, frequency,
                                  signal_strength, battery_voltage):
        """Apply data quality issues"""
        cumulative = 0
        
        for issue, prob in self.issue_probabilities.items():
            cumulative += prob
            if rand_val < cumulative:
                if issue == 'missing_reading':
                    return consumption, cumulative_reading, voltage, frequency, signal_strength, battery_voltage, 'Missing Reading'
                elif issue == 'negative_reading':
                    return -consumption, previous_reading - consumption, voltage, frequency, signal_strength, battery_voltage, 'Negative Reading'
                elif issue == 'zero_reading':
                    return 0, previous_reading, voltage, frequency, signal_strength, battery_voltage, 'Zero Reading'
                elif issue == 'abnormal_spike':
                    return consumption * 10, cumulative_reading + consumption * 9, voltage, frequency, signal_strength, battery_voltage, 'Abnormal Spike'
                elif issue == 'voltage_sag':
                    return consumption, cumulative_reading, voltage * 0.7, frequency, signal_strength, battery_voltage, 'Voltage Sag'
                elif issue == 'frequency_variation':
                    return consumption, cumulative_reading, voltage, 47.5, signal_strength, battery_voltage, 'Frequency Variation'
                elif issue == 'signal_drop':
                    return consumption, cumulative_reading, voltage, frequency, -105, battery_voltage, 'Signal Drop'
                elif issue == 'battery_fault':
                    return consumption, cumulative_reading, voltage, frequency, signal_strength, 2.8, 'Battery Fault'
                elif issue == 'meter_tamper':
                    return consumption * 0.3, previous_reading + consumption * 0.3, voltage, frequency, signal_strength, battery_voltage, 'Meter Tamper'
                elif issue == 'reverse_energy':
                    return -consumption, previous_reading - consumption, voltage, frequency, signal_strength, battery_voltage, 'Reverse Energy'
        
        return consumption, cumulative_reading, voltage, frequency, signal_strength, battery_voltage, 'Normal'

    def save_monthly_readings(self, readings_df: pd.DataFrame, output_dir: str):
        """Save readings in monthly folders by meter"""
        
        readings_df['year_month'] = pd.to_datetime(readings_df['timestamp']).dt.strftime('%Y-%m')
        
        print("\nSaving readings by meter/month...")
        
        for (meter_number, year_month), group in tqdm(
            readings_df.groupby(['meter_number', 'year_month']),
            desc="Writing files"
        ):
            meter_dir = os.path.join(output_dir, 'readings', meter_number)
            os.makedirs(meter_dir, exist_ok=True)
            
            filename = f"{meter_number}_{year_month}.csv"
            filepath = os.path.join(meter_dir, filename)
            
            group.drop(columns=['year_month'], errors='ignore').to_csv(filepath, index=False)

    def generate_monthly_bills_dynamic(self,
                                     meters_df: pd.DataFrame,
                                     readings_df: pd.DataFrame,
                                     start_date: str,
                                     end_date: str) -> pd.DataFrame:
        """Generate monthly bills considering meter lifecycle"""
        
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        
        billing_months = pd.date_range(start=start, end=end, freq='MS')
        bills = []
        
        print("\nGenerating monthly bills...")
        
        # Group readings by meter
        for meter_number, meter_readings in tqdm(readings_df.groupby('meter_number'), desc="Processing meters"):
            # Get meter info
            meter_info = meters_df[meters_df['meter_number'] == meter_number].iloc[0]
            
            meter_readings['timestamp'] = pd.to_datetime(meter_readings['timestamp'])
            
            for billing_month in billing_months:
                month_str = billing_month.strftime('%b %y').upper()
                next_month = billing_month + pd.DateOffset(months=1)
                
                # Get readings for this month
                month_readings = meter_readings[
                    (meter_readings['timestamp'] >= billing_month) &
                    (meter_readings['timestamp'] < next_month)
                ]
                
                if len(month_readings) == 0:
                    continue
                
                # Calculate consumption (handle data quality)
                valid_readings = month_readings[month_readings['data_quality_flag'] == 'Normal']
                
                if len(valid_readings) > 1:
                    first = valid_readings.iloc[0]['reading_kwh']
                    last = valid_readings.iloc[-1]['reading_kwh']
                    
                    if last >= first:
                        consumption = last - first
                    else:
                        consumption = valid_readings['energy_consumed_kwh'].sum()
                else:
                    consumption = month_readings['energy_consumed_kwh'].sum()
                
                consumption = max(0, consumption)
                
                # Calculate bill
                bill = self._calculate_bill(
                    consumption,
                    meter_info['tariff_category'],
                    meter_info['connected_load_kw'],
                    month_str,
                    meter_number,
                    meter_info['consumer_id'],
                    meter_info['name'],
                    meter_info['address'],
                    meter_info['distribution_transformer_id']
                )
                
                # Add meter generation info
                bill['meter_generation'] = meter_info['meter_generation']
                bill['has_solar'] = meter_info['has_solar']
                
                bills.append(bill)
        
        return pd.DataFrame(bills)

    def _calculate_bill(self, consumption_kwh, tariff, connected_load,
                       billing_month, meter_number, consumer_id,
                       consumer_name, address, transformer_id):
        """Calculate electricity bill"""
        
        # Tariff rates (as per IESCO)
        if 'A-1' in tariff:
            slabs = [(100, 5.79), (200, 8.11), (300, 10.20), (400, 16.00), (500, 18.00), (float('inf'), 21.00)]
            fixed = 50 if connected_load < 5 else 100
        elif 'A-2' in tariff:
            slabs = [(100, 16.00), (300, 18.00), (float('inf'), 21.00)]
            fixed = 250 * connected_load
        elif 'B-1' in tariff:
            slabs = [(float('inf'), 14.00)]
            fixed = 200 * connected_load
        elif 'B-2' in tariff:
            slabs = [(float('inf'), 16.00)]
            fixed = 300 * connected_load
        elif 'C-1' in tariff:
            slabs = [(float('inf'), 12.00)]
            fixed = 100 * connected_load
        else:
            slabs = [(float('inf'), 18.00)]
            fixed = 400 * connected_load
        
        # Calculate variable
        remaining = consumption_kwh
        variable = 0
        for limit, rate in slabs:
            if remaining <= 0:
                break
            slab_units = min(remaining, limit)
            variable += slab_units * rate
            remaining -= slab_units
        
        # Taxes
        gst = (variable + fixed) * 0.18
        duty = variable * 0.015
        tv_fee = 35 if random.random() > 0.7 else 0
        late = 0 if random.random() > 0.1 else (variable + fixed) * 0.05
        
        total = variable + fixed + gst + duty + tv_fee + late
        
        # Dates
        billing_date = pd.to_datetime(f"25 {billing_month}")
        issue_date = billing_date - timedelta(days=5)
        due_date = billing_date + timedelta(days=14)
        
        return {
            'bill_id': f"BILL{datetime.now().strftime('%Y%m%d%H%M%S')}{random.randint(100, 999)}",
            'meter_number': meter_number,
            'consumer_id': consumer_id,
            'consumer_name': consumer_name,
            'address': address,
            'distribution_transformer_id': transformer_id,
            'billing_month': billing_month,
            'issue_date': issue_date.strftime('%Y-%m-%d'),
            'due_date': due_date.strftime('%Y-%m-%d'),
            'units_consumed': round(consumption_kwh, 2),
            'variable_charges': round(variable, 2),
            'fixed_charges': round(fixed, 2),
            'gst': round(gst, 2),
            'electricity_duty': round(duty, 2),
            'tv_fee': round(tv_fee, 2),
            'late_payment_surcharge': round(late, 2),
            'total_amount': round(total, 2),
            'amount_within_due_date': round(total, 2),
            'amount_after_due_date': round(total * 1.05, 2),
            'tariff_applied': tariff,
            'reference_no': f"11 {random.randint(10000, 99999)} {random.randint(1000000, 9999999)} U",
            'payment_status': random.choice(['Unpaid', 'Paid', 'Overdue']) if random.random() > 0.4 else 'Paid'
        }

    def generate_all_data_dynamic(self,
                                 initial_meters: int,
                                 start_date: str,
                                 end_date: str,
                                 reading_frequency: int = 15,
                                 output_dir: str = './iesco_dynamic_data'):
        """
        Main method to generate complete dynamic dataset
        """
        
        print("="*70)
        print("IESCO DYNAMIC SMART METER DATA GENERATOR")
        print("="*70)
        
        os.makedirs(output_dir, exist_ok=True)
        
        # Step 1: Generate initial infrastructure
        print("\n📊 Step 1: Generating initial transformer infrastructure...")
        transformers_df = self.generate_initial_transformers(target_dist_transformers=1500)
        transformers_df.to_csv(os.path.join(output_dir, 'transformers_initial.csv'), index=False)
        print(f"   ✅ Generated {len(transformers_df)} transformers")
        
        # Step 2: Generate initial meters
        print(f"\n👥 Step 2: Generating {initial_meters} initial meters...")
        meters_df = self.generate_initial_meters(initial_meters, transformers_df, start_date)
        meters_df.to_csv(os.path.join(output_dir, 'meters_initial.csv'), index=False)
        print(f"   ✅ Generated {len(meters_df)} initial meters")
        
        # Step 3: Simulate monthly events
        print("\n🔄 Step 3: Simulating monthly events (new connections, replacements, failures)...")
        
        all_events = []
        current_meters = meters_df.copy()
        current_transformers = transformers_df.copy()
        
        # Get all months between start and end
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        months = pd.date_range(start=start, end=end, freq='MS')
        
        for month_idx, month_start in enumerate(tqdm(months, desc="Processing months")):
            # Simulate events for this month
            current_meters, current_transformers, month_events = self.simulate_monthly_events(
                current_meters, current_transformers, month_start, month_idx
            )
            all_events.extend(month_events)
            
            # Save monthly snapshots
            if month_idx % 3 == 0:  # Save quarterly snapshots
                meters_df.to_csv(os.path.join(output_dir, f'meters_snapshot_{month_start.strftime("%Y%m")}.csv'), index=False)
        
        # Save final meters and events
        current_meters.to_csv(os.path.join(output_dir, 'meters_final.csv'), index=False)
        
        events_df = pd.DataFrame(all_events)
        events_df.to_csv(os.path.join(output_dir, 'lifecycle_events.csv'), index=False)
        
        print(f"   ✅ Simulated {len(months)} months, {len(all_events)} events")
        print(f"      - Final meters: {len(current_meters[current_meters['is_active']])} active")
        print(f"      - New connections: {len([e for e in all_events if e['event_type'] == 'new_connection'])}")
        print(f"      - Meter replacements: {len([e for e in all_events if e['event_type'] == 'meter_replacement'])}")
        print(f"      - Transformer upgrades: {len([e for e in all_events if e['event_type'] == 'transformer_upgrade'])}")
        
        # Step 4: Generate readings with dynamic events
        print(f"\n📈 Step 4: Generating meter readings ({reading_frequency} min intervals)...")
        readings_df = self.generate_readings_dynamic(
            current_meters, current_transformers, all_events,
            start_date, end_date, reading_frequency
        )
        
        # Save readings summary
        readings_df.to_csv(os.path.join(output_dir, 'readings_summary.csv'), index=False)
        
        # Save in monthly folder structure
        self.save_monthly_readings(readings_df, output_dir)
        print(f"   ✅ Generated {len(readings_df):,} total readings")
        
        # Step 5: Generate monthly bills
        print(f"\n💰 Step 5: Generating monthly bills...")
        bills_df = self.generate_monthly_bills_dynamic(
            current_meters, readings_df, start_date, end_date
        )
        bills_df.to_csv(os.path.join(output_dir, 'bills.csv'), index=False)
        print(f"   ✅ Generated {len(bills_df):,} bills")
        
        # Step 6: Generate summary statistics
        print(f"\n📋 Step 6: Generating summary statistics...")
        summary = self._generate_summary(
            current_meters, readings_df, bills_df, current_transformers, all_events
        )
        
        with open(os.path.join(output_dir, 'dataset_summary.json'), 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        
        # Print final summary
        self._print_summary(summary, output_dir)
        
        return {
            'meters': current_meters,
            'transformers': current_transformers,
            'readings': readings_df,
            'bills': bills_df,
            'events': events_df,
            'summary': summary
        }

    def _generate_summary(self, meters_df, readings_df, bills_df, transformers_df, events):
        """Generate comprehensive summary"""
        
        active_meters = meters_df[meters_df['is_active']]
        
        return {
            'generation_timestamp': datetime.now().isoformat(),
            'infrastructure': {
                'total_transformers': len(transformers_df),
                'grid_transformers': len(transformers_df[transformers_df['transformer_type'] == 'grid']),
                'distribution_transformers': len(transformers_df[transformers_df['transformer_type'] == 'distribution']),
                'total_capacity_mva': transformers_df[transformers_df['transformer_type'] == 'grid']['rating_mva'].sum(),
                'upgraded_transformers': len(transformers_df[transformers_df['upgrade_date'].notna()])
            },
            'meters': {
                'total_meters': len(meters_df),
                'active_meters': len(active_meters),
                'by_category': active_meters['consumer_category'].value_counts().to_dict(),
                'smart_meters': len(active_meters[active_meters['meter_type'] == 'Smart']),
                'solar_meters': len(active_meters[active_meters['has_solar']]),
                'meter_generations': active_meters['meter_generation'].value_counts().to_dict()
            },
            'readings': {
                'total': len(readings_df),
                'date_range': [readings_df['timestamp'].min(), readings_df['timestamp'].max()],
                'data_quality': readings_df['data_quality_flag'].value_counts().to_dict(),
                'total_energy_kwh': readings_df['energy_consumed_kwh'].sum()
            },
            'bills': {
                'total': len(bills_df),
                'total_amount': float(bills_df['total_amount'].sum()),
                'avg_amount': float(bills_df['total_amount'].mean()),
                'total_units': float(bills_df['units_consumed'].sum())
            },
            'events': {
                'total': len(events),
                'by_type': pd.DataFrame(events)['event_type'].value_counts().to_dict() if len(events) > 0 else {}
            }
        }

    def _print_summary(self, summary, output_dir):
        """Print final summary"""
        
        print("\n" + "="*70)
        print("✅ DATA GENERATION COMPLETE")
        print("="*70)
        
        print("\n📁 OUTPUT DIRECTORY:")
        print(f"   {output_dir}/")
        print(f"   ├── transformers_initial.csv")
        print(f"   ├── meters_initial.csv")
        print(f"   ├── lifecycle_events.csv")
        print(f"   ├── meters_final.csv")
        print(f"   ├── readings_summary.csv")
        print(f"   ├── readings/")
        print(f"   │   ├── [meter_number]/")
        print(f"   │   │   └── [meter_number]_[YYYY-MM].csv")
        print(f"   ├── bills.csv")
        print(f"   └── dataset_summary.json")
        
        print("\n📊 FINAL STATISTICS:")
        print(f"   Infrastructure:")
        print(f"   - Grid Transformers: {summary['infrastructure']['grid_transformers']}")
        print(f"   - Distribution Transformers: {summary['infrastructure']['distribution_transformers']}")
        print(f"   - Upgraded Transformers: {summary['infrastructure']['upgraded_transformers']}")
        
        print(f"\n   Meters:")
        print(f"   - Total (all time): {summary['meters']['total_meters']}")
        print(f"   - Active at end: {summary['meters']['active_meters']}")
        for cat, count in summary['meters']['by_category'].items():
            print(f"     * {cat.title()}: {count}")
        print(f"   - Smart Meters: {summary['meters']['smart_meters']}")
        print(f"   - Solar: {summary['meters']['solar_meters']}")
        
        print(f"\n   Lifecycle Events:")
        for event_type, count in summary['events']['by_type'].items():
            print(f"   - {event_type}: {count}")
        
        print(f"\n   Readings:")
        print(f"   - Total: {summary['readings']['total']:,}")
        print(f"   - Energy: {summary['readings']['total_energy_kwh']:,.0f} kWh")
        
        print(f"\n   Data Quality Issues:")
        for flag, count in list(summary['readings']['data_quality'].items())[:5]:
            pct = (count / summary['readings']['total']) * 100
            print(f"   - {flag}: {pct:.2f}%")
        
        print("\n" + "="*70)


def main():
    parser = argparse.ArgumentParser(description='Generate Dynamic IESCO Smart Meter Data')
    parser.add_argument('--initial_meters', type=int, default=5000,
                       help='Initial number of meters')
    parser.add_argument('--start_date', type=str, default='2023-01-01',
                       help='Start date')
    parser.add_argument('--end_date', type=str, default='2025-01-31',
                       help='End date')
    parser.add_argument('--frequency', type=int, default=15,
                       help='Reading frequency in minutes')
    parser.add_argument('--output_dir', type=str, default='./iesco_dynamic_data',
                       help='Output directory')
    
    args = parser.parse_args()
    
    generator = IESCODynamicDataGenerator()
    
    data = generator.generate_all_data_dynamic(
        initial_meters=args.initial_meters,
        start_date=args.start_date,
        end_date=args.end_date,
        reading_frequency=args.frequency,
        output_dir=args.output_dir
    )


if __name__ == "__main__":
    main()