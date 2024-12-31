"""
Enhanced IESCO Smart Meter Data Generator with Complete Regional Coverage
Covers all 5 districts, 20+ divisions, and 50+ sub-divisions as per official IESCO territory
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

# Initialize Faker with Pakistani locale
fake = Faker('en_PK')
Faker.seed(42)
np.random.seed(42)
random.seed(42)

class IESCOCompleteRegionGenerator:
    def __init__(self):
        # Tariff categories as per IESCO
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
        
        # COMPLETE IESCO TERRITORY - 5 Districts with all divisions/sub-divisions
        # Based on search results [citation:2][citation:4][citation:5]
        self.districts = {
            'ISLAMABAD': {
                'division_headquarters': ['G-7/4 ISLAMABAD'],
                'growth_rate': 0.18,  # Highest growth - capital city
                'population_density': 'very_high',
                'consumer_density': 500,  # consumers per sq km
                'coordinates': {'lat_center': 33.6844, 'lon_center': 73.0479, 'radius': 0.15},
                'divisions': {
                    'ISLAMABAD DIVISION 1': {
                        'sub_divisions': [
                            'I-10 SUB DIVISION', 'G-10 SUB DIVISION', 'F-11 SUB DIVISION',
                            'I-8 SUB DIVISION', 'G-9 SUB DIVISION', 'F-10 SUB DIVISION',
                            'I-9 SUB DIVISION', 'G-8 SUB DIVISION', 'F-9 SUB DIVISION',
                            'I-11 SUB DIVISION', 'G-11 SUB DIVISION'
                        ],
                        'grid_stations': ['132kV I-10', '132kV G-10', '220kV ISLAMABAD WEST'],
                        'consumer_mix': {'residential': 0.7, 'commercial': 0.25, 'industrial': 0.03, 'agricultural': 0.01, 'bulk': 0.01}
                    },
                    'ISLAMABAD DIVISION 2': {
                        'sub_divisions': [
                            'I-16 SUB DIVISION', 'I-17 SUB DIVISION', 'KORANG TOWN SUB DIVISION',
                            'TARLAI SUB DIVISION', 'NILORE SUB DIVISION', 'SHAHZAD TOWN SUB DIVISION',
                            'BHARA KAHU SUB DIVISION'
                        ],
                        'grid_stations': ['132kV I-16', '132kV TARLAI', '220kV ISLAMABAD EAST'],
                        'consumer_mix': {'residential': 0.65, 'commercial': 0.2, 'industrial': 0.1, 'agricultural': 0.03, 'bulk': 0.02}
                    },
                    'BARAKAHU DIVISION': {
                        'sub_divisions': [
                            'BARAKAHU SUB DIVISION', 'MURREE SUB DIVISION', 'KOHAT BAZAR SUB DIVISION'
                        ],
                        'grid_stations': ['132kV BARAKAHU', '132kV MURREE HILLS'],
                        'consumer_mix': {'residential': 0.75, 'commercial': 0.2, 'industrial': 0.02, 'agricultural': 0.02, 'bulk': 0.01}
                    }
                }
            },
            
            'RAWALPINDI': {
                'division_headquarters': ['SATELLITE TOWN RAWALPINDI'],
                'growth_rate': 0.15,
                'population_density': 'high',
                'consumer_density': 400,
                'coordinates': {'lat_center': 33.5651, 'lon_center': 73.0169, 'radius': 0.2},
                'divisions': {
                    'RAWALPINDI CITY DIVISION': {
                        'sub_divisions': [
                            'BABRI BAZAR SUB DIVISION', 'PIRWADHAI SUB DIVISION', 'DHAK RIATA SUB DIVISION',
                            'WARIS KHAN SUB DIVISION', 'CHAHDAR BAZAR SUB DIVISION', 'KALYAN SUB DIVISION'
                        ],
                        'grid_stations': ['132kV CITY RAWALPINDI', '132kV WARIS KHAN'],
                        'consumer_mix': {'residential': 0.6, 'commercial': 0.35, 'industrial': 0.03, 'agricultural': 0.01, 'bulk': 0.01}
                    },
                    'CANTT DIVISION': {
                        'sub_divisions': [
                            'CANTT SUB DIVISION RAWALPINDI', 'TARIQABAD SUB DIVISION',
                            'RA BAZAR SUB DIVISION', 'CHAKLALA SUB DIVISION'
                        ],
                        'grid_stations': ['132kV CANTT', '132kV CHAKLALA AIRPORT'],
                        'consumer_mix': {'residential': 0.7, 'commercial': 0.25, 'industrial': 0.03, 'agricultural': 0.01, 'bulk': 0.01}
                    },
                    'SATELLITE TOWN DIVISION': {
                        'sub_divisions': [
                            'SATELLITE TOWN MAIN SUB DIVISION', 'MUSLIM TOWN SUB DIVISION',
                            'GANGAL SUB DIVISION', 'DHOKE KALA KHAN SUB DIVISION'
                        ],
                        'grid_stations': ['132kV SATELLITE TOWN', '132kV MUSLIM TOWN'],
                        'consumer_mix': {'residential': 0.65, 'commercial': 0.3, 'industrial': 0.03, 'agricultural': 0.01, 'bulk': 0.01}
                    },
                    'WESTRIDGE DIVISION': {
                        'sub_divisions': [
                            'WESTRIDGE MAIN SUB DIVISION', 'TARNOL SUB DIVISION',
                            'DEFENCE WESTRIDGE SUB DIVISION', 'SHAMSABAD SUB DIVISION'
                        ],
                        'grid_stations': ['132kV WESTRIDGE', '220kV RAWALPINDI WEST'],
                        'consumer_mix': {'residential': 0.7, 'commercial': 0.25, 'industrial': 0.03, 'agricultural': 0.01, 'bulk': 0.01}
                    },
                    'RAWAT DIVISION': {
                        'sub_divisions': [
                            'RAWAT SUB DIVISION', 'MANDRA SUB DIVISION',
                            'KALLAR SYEDAN SUB DIVISION', 'CHAK BELI KHAN SUB DIVISION'
                        ],
                        'grid_stations': ['132kV RAWAT', '132kV MANDRA'],
                        'consumer_mix': {'residential': 0.6, 'commercial': 0.2, 'industrial': 0.1, 'agricultural': 0.08, 'bulk': 0.02}
                    }
                }
            },
            
            'ATTOCK': {
                'division_headquarters': ['ATTOCK CITY'],
                'growth_rate': 0.10,
                'population_density': 'medium',
                'consumer_density': 200,
                'coordinates': {'lat_center': 33.7667, 'lon_center': 72.3667, 'radius': 0.25},
                'divisions': {
                    'ATTOCK DIVISION': {
                        'sub_divisions': [
                            'ATTOCK MAIN SUB DIVISION', 'SHADI KHAN SUB DIVISION',
                            'HASSANABDAL SUB DIVISION', 'BURHAN SUB DIVISION'
                        ],
                        'grid_stations': ['132kV ATTOCK CITY', '132kV HASSANABDAL'],
                        'consumer_mix': {'residential': 0.55, 'commercial': 0.2, 'industrial': 0.1, 'agricultural': 0.13, 'bulk': 0.02}
                    },
                    'TAXILA DIVISION': {
                        'sub_divisions': [
                            'TAXILA MAIN SUB DIVISION', 'MARGALLA SUB DIVISION',
                            'WAH CANTT SUB DIVISION', 'NEW TAXILA SUB DIVISION'
                        ],
                        'grid_stations': ['132kV TAXILA', '132kV WAH INDUSTRIAL'],
                        'consumer_mix': {'residential': 0.5, 'commercial': 0.15, 'industrial': 0.25, 'agricultural': 0.08, 'bulk': 0.02}
                    },
                    'PINDIGHEB DIVISION': {
                        'sub_divisions': [
                            'PINDIGHEB MAIN SUB DIVISION', 'JAN SUB DIVISION',
                            'FAATEH JANG SUB DIVISION'
                        ],
                        'grid_stations': ['132kV PINDIGHEB'],
                        'consumer_mix': {'residential': 0.5, 'commercial': 0.1, 'industrial': 0.05, 'agricultural': 0.33, 'bulk': 0.02}
                    }
                }
            },
            
            'JHELUM': {
                'division_headquarters': ['JHELUM CITY'],
                'growth_rate': 0.10,
                'population_density': 'medium',
                'consumer_density': 180,
                'coordinates': {'lat_center': 32.9333, 'lon_center': 73.7333, 'radius': 0.2},
                'divisions': {
                    'JHELUM DIVISION 1': {
                        'sub_divisions': [
                            'JHELUM MAIN SUB DIVISION', 'SARA-E-ALAMGIR SUB DIVISION',
                            'KHUSHAB SUB DIVISION'
                        ],
                        'grid_stations': ['132kV JHELUM CITY', '132kV SARA-E-ALAMGIR'],
                        'consumer_mix': {'residential': 0.6, 'commercial': 0.2, 'industrial': 0.1, 'agricultural': 0.08, 'bulk': 0.02}
                    },
                    'JHELUM DIVISION 2': {
                        'sub_divisions': [
                            'DINA SUB DIVISION', 'SOHAWA SUB DIVISION',
                            'PIND DADAN KHAN SUB DIVISION'
                        ],
                        'grid_stations': ['132kV DINA', '132kV PIND DADAN KHAN'],
                        'consumer_mix': {'residential': 0.5, 'commercial': 0.1, 'industrial': 0.05, 'agricultural': 0.33, 'bulk': 0.02}
                    },
                    'GUJAR KHAN DIVISION': {
                        'sub_divisions': [
                            'GUJAR KHAN MAIN SUB DIVISION', 'KALAR SYEDAN SUB DIVISION',
                            'MANDRA SUB DIVISION', 'NAROLI SUB DIVISION'
                        ],
                        'grid_stations': ['132kV GUJAR KHAN', '132kV KALAR SYEDAN'],
                        'consumer_mix': {'residential': 0.55, 'commercial': 0.15, 'industrial': 0.1, 'agricultural': 0.18, 'bulk': 0.02}
                    }
                }
            },
            
            'CHAKWAL': {
                'division_headquarters': ['CHAKWAL CITY'],
                'growth_rate': 0.08,
                'population_density': 'low',
                'consumer_density': 120,
                'coordinates': {'lat_center': 32.9333, 'lon_center': 72.8500, 'radius': 0.25},
                'divisions': {
                    'CHAKWAL DIVISION': {
                        'sub_divisions': [
                            'CHAKWAL MAIN SUB DIVISION', 'KALAR KAHAR SUB DIVISION',
                            'CHOA SAIDAN SHAH SUB DIVISION'
                        ],
                        'grid_stations': ['132kV CHAKWAL CITY', '132kV KALAR KAHAR'],
                        'consumer_mix': {'residential': 0.5, 'commercial': 0.1, 'industrial': 0.05, 'agricultural': 0.33, 'bulk': 0.02}
                    },
                    'TALAGANG DIVISION': {
                        'sub_divisions': [
                            'TALAGANG MAIN SUB DIVISION', 'DANDA SHAH BALAWAL SUB DIVISION',
                            'LAWA SUB DIVISION'
                        ],
                        'grid_stations': ['132kV TALAGANG', '132kV LAWA'],
                        'consumer_mix': {'residential': 0.45, 'commercial': 0.1, 'industrial': 0.03, 'agricultural': 0.4, 'bulk': 0.02}
                    },
                    'DHUDIAL DIVISION': {
                        'sub_divisions': [
                            'DHUDIAL MAIN SUB DIVISION', 'CHAK BELI KHAN SUB DIVISION'
                        ],
                        'grid_stations': ['132kV DHUDIAL'],
                        'consumer_mix': {'residential': 0.45, 'commercial': 0.1, 'industrial': 0.02, 'agricultural': 0.41, 'bulk': 0.02}
                    },
                    'PIND DADAN KHAN DIVISION': {
                        'sub_divisions': [
                            'PIND DADAN KHAN MAIN SUB DIVISION', 'JHELUM REFERRAL SUB DIVISION'
                        ],
                        'grid_stations': ['132kV PIND DADAN KHAN'],
                        'consumer_mix': {'residential': 0.4, 'commercial': 0.1, 'industrial': 0.02, 'agricultural': 0.46, 'bulk': 0.02}
                    }
                }
            }
        }
        
        # Flatten all sub-divisions for easy lookup
        self.all_sub_divisions = []
        self.sub_division_to_info = {}
        
        for district_name, district_info in self.districts.items():
            for div_name, div_info in district_info['divisions'].items():
                for sub_div in div_info['sub_divisions']:
                    self.all_sub_divisions.append({
                        'district': district_name,
                        'division': div_name,
                        'sub_division': sub_div,
                        'grid_stations': div_info['grid_stations'],
                        'consumer_mix': div_info['consumer_mix'],
                        'coordinates': district_info['coordinates'],
                        'growth_rate': district_info['growth_rate']
                    })
                    self.sub_division_to_info[sub_div] = {
                        'district': district_name,
                        'division': div_name,
                        'grid_stations': div_info['grid_stations'],
                        'consumer_mix': div_info['consumer_mix']
                    }
        
        # Transformer specifications
        self.transformer_specs = {
            'grid_transformer': {
                'types': [
                    {'rating_mva': 250, 'voltage': '220/132kV', 'count': 8, 'upgrade_to': 400},
                    {'rating_mva': 160, 'voltage': '132/33kV', 'count': 25, 'upgrade_to': 250},
                    {'rating_mva': 100, 'voltage': '132/11kV', 'count': 40, 'upgrade_to': 160}
                ]
            },
            'distribution_transformer': {
                'types': [
                    {'rating_kva': 1500, 'voltage': '11/0.4kV', 'phase': '3-phase', 'count': 80, 'upgrade_to': 2000},
                    {'rating_kva': 1000, 'voltage': '11/0.4kV', 'phase': '3-phase', 'count': 200, 'upgrade_to': 1500},
                    {'rating_kva': 750, 'voltage': '11/0.4kV', 'phase': '3-phase', 'count': 300, 'upgrade_to': 1000},
                    {'rating_kva': 500, 'voltage': '11/0.4kV', 'phase': '3-phase', 'count': 400, 'upgrade_to': 750},
                    {'rating_kva': 250, 'voltage': '11/0.4kV', 'phase': '1-phase', 'count': 800, 'upgrade_to': 400},
                    {'rating_kva': 100, 'voltage': '11/0.4kV', 'phase': '1-phase', 'count': 1200, 'upgrade_to': 200}
                ]
            }
        }
        
        # Event probabilities
        self.event_probabilities = {
            'new_connection': 0.015,
            'meter_replacement': 0.005,
            'meter_failure': 0.003,
            'transformer_upgrade': 0.001,
            'consumer_churn': 0.002,
            'tariff_change': 0.001
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
        
        self.events_log = []

    def generate_region_transformers(self, target_dist_transformers: int = 2500) -> pd.DataFrame:
        """
        Generate transformers across all 5 districts with proper distribution
        Based on population density and consumer mix
        """
        transformers = []
        transformer_id = 1
        
        print("\n📊 Generating Grid Transformers by District...")
        
        # 1. Generate Grid Transformers (one per major grid station)
        for district_name, district_info in self.districts.items():
            district_grid_stations = set()
            for div_name, div_info in district_info['divisions'].items():
                for station in div_info['grid_stations']:
                    district_grid_stations.add(station)
            
            print(f"   {district_name}: {len(district_grid_stations)} grid stations")
            
            for grid_station in district_grid_stations:
                # Each grid station has 2-4 grid transformers
                num_grid_trans = random.randint(2, 4)
                for i in range(num_grid_trans):
                    trans_type = random.choice(self.transformer_specs['grid_transformer']['types'])
                    install_date = fake.date_between(start_date='-20y', end_date='-5y')
                    
                    # Generate coordinates based on district center
                    lat = district_info['coordinates']['lat_center'] + random.uniform(-0.05, 0.05)
                    lon = district_info['coordinates']['lon_center'] + random.uniform(-0.05, 0.05)
                    
                    trans = {
                        'transformer_id': f'GT{transformer_id:06d}',
                        'transformer_type': 'grid',
                        'district': district_name,
                        'grid_station': grid_station,
                        'rating_mva': trans_type['rating_mva'],
                        'initial_rating_mva': trans_type['rating_mva'],
                        'voltage_level': trans_type['voltage'],
                        'manufacturer': random.choice(['Siemens', 'ABB', 'Heavy Electrical Complex', 'Toshiba']),
                        'installation_date': install_date,
                        'last_maintenance': fake.date_between(start_date='-1y', end_date='-1d'),
                        'upgrade_date': None,
                        'upgrade_history': [],
                        'latitude': lat,
                        'longitude': lon,
                        'status': 'Active',
                        'capacity_utilization_pct': random.uniform(40, 90) * 
                                                     (1.2 if district_info['population_density'] == 'very_high' else
                                                      1.1 if district_info['population_density'] == 'high' else 1.0)
                    }
                    transformers.append(trans)
                    transformer_id += 1
        
        # 2. Generate Distribution Transformers (proportional to population density)
        print("\n📊 Generating Distribution Transformers by Sub-Division...")
        
        # Calculate transformer distribution
        total_consumers_estimate = 5000000  # ~5 million consumers across IESCO
        density_factors = {
            'very_high': 1.5,
            'high': 1.2,
            'medium': 1.0,
            'low': 0.7
        }
        
        # First pass: calculate weights for each sub-division
        sub_div_weights = []
        for sub_div_info in self.all_sub_divisions:
            district_info = self.districts[sub_div_info['district']]
            weight = density_factors[district_info['population_density']] * \
                    district_info['consumer_density']
            sub_div_weights.append((sub_div_info, weight))
        
        total_weight = sum(w for _, w in sub_div_weights)
        
        # Distribute transformers
        for sub_div_info, weight in sub_div_weights:
            # Number of transformers proportional to weight
            num_transformers = max(1, int((weight / total_weight) * target_dist_transformers))
            
            # Find grid transformers in this district
            district_grid_transformers = [t for t in transformers 
                                        if t['transformer_type'] == 'grid' 
                                        and t['district'] == sub_div_info['district']]
            
            for _ in range(num_transformers):
                # Select grid transformer (random or closest)
                grid_trans = random.choice(district_grid_transformers) if district_grid_transformers else None
                
                # Select transformer type based on consumer mix
                consumer_mix = sub_div_info['consumer_mix']
                
                # More industrial areas need larger transformers
                industrial_factor = consumer_mix.get('industrial', 0) * 2
                commercial_factor = consumer_mix.get('commercial', 0) * 1.5
                size_factor = 1 + industrial_factor + commercial_factor
                
                # Choose transformer size
                if size_factor > 2.0 and random.random() > 0.3:
                    trans_type = random.choice([t for t in self.transformer_specs['distribution_transformer']['types'] 
                                              if t['rating_kva'] >= 750])
                elif size_factor > 1.5:
                    trans_type = random.choice([t for t in self.transformer_specs['distribution_transformer']['types'] 
                                              if t['rating_kva'] >= 500])
                else:
                    trans_type = random.choice(self.transformer_specs['distribution_transformer']['types'])
                
                # Generate coordinates within sub-division area
                lat = sub_div_info['coordinates']['lat_center'] + random.uniform(-0.03, 0.03)
                lon = sub_div_info['coordinates']['lon_center'] + random.uniform(-0.03, 0.03)
                
                install_date = fake.date_between(start_date='-15y', end_date='-1y')
                
                trans = {
                    'transformer_id': f'DT{transformer_id:06d}',
                    'transformer_type': 'distribution',
                    'district': sub_div_info['district'],
                    'division': sub_div_info['division'],
                    'sub_division': sub_div_info['sub_division'],
                    'feeder_name': f"FD{random.randint(1000, 9999)}_{sub_div_info['sub_division'][:10]}",
                    'grid_transformer_id': grid_trans['transformer_id'] if grid_trans else None,
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
                    'latitude': lat,
                    'longitude': lon,
                    'status': 'Active',
                    'capacity_utilization_pct': random.uniform(30, 85),
                    'consumers_connected': 0
                }
                transformers.append(trans)
                transformer_id += 1
        
        print(f"\n✅ Generated {len(transformers)} transformers across {len(self.districts)} districts")
        print(f"   - Grid Transformers: {len([t for t in transformers if t['transformer_type'] == 'grid'])}")
        print(f"   - Distribution Transformers: {len([t for t in transformers if t['transformer_type'] == 'distribution'])}")
        
        return pd.DataFrame(transformers)

    def generate_region_meters(self,
                              num_meters: int,
                              transformers_df: pd.DataFrame,
                              current_date: str) -> pd.DataFrame:
        """
        Generate meters across all sub-divisions with realistic distribution
        """
        
        meters = []
        distribution_transformers = transformers_df[transformers_df['transformer_type'] == 'distribution']
        
        print("\n👥 Generating meters across all sub-divisions...")
        
        # Group transformers by sub-division
        transformers_by_subdiv = defaultdict(list)
        for _, transformer in distribution_transformers.iterrows():
            transformers_by_subdiv[transformer['sub_division']].append(transformer)
        
        # Calculate consumer distribution based on sub-division characteristics
        sub_div_targets = {}
        total_weight = 0
        
        for sub_div_info in self.all_sub_divisions:
            sub_div = sub_div_info['sub_division']
            district_info = self.districts[sub_div_info['district']]
            
            # Weight factors: population density, number of transformers, consumer mix
            density_factor = {
                'very_high': 3.0,
                'high': 2.0,
                'medium': 1.0,
                'low': 0.5
            }[district_info['population_density']]
            
            transformer_count = len(transformers_by_subdiv.get(sub_div, []))
            transformer_factor = min(2.0, transformer_count / 10)  # Cap at 2x
            
            # Commercial/industrial areas get more meters per transformer
            commercial_factor = 1 + sub_div_info['consumer_mix'].get('commercial', 0) * 2
            industrial_factor = 1 + sub_div_info['consumer_mix'].get('industrial', 0) * 3
            
            weight = density_factor * (1 + transformer_factor) * commercial_factor * industrial_factor
            sub_div_targets[sub_div] = {
                'weight': weight,
                'info': sub_div_info
            }
            total_weight += weight
        
        # Distribute meters proportionally
        meters_allocated = 0
        meters_by_subdiv = defaultdict(list)
        
        # First pass: allocate base meters to each sub-division
        for sub_div, target_info in sub_div_targets.items():
            sub_div_meters = max(1, int((target_info['weight'] / total_weight) * num_meters))
            meters_allocated += sub_div_meters
            meters_by_subdiv[sub_div] = {'count': sub_div_meters, 'meters': []}
        
        # Adjust for rounding errors
        if meters_allocated < num_meters:
            # Add remaining to largest sub-divisions
            extra = num_meters - meters_allocated
            sorted_subdivs = sorted(sub_div_targets.items(), key=lambda x: x[1]['weight'], reverse=True)
            for i in range(extra):
                sub_div = sorted_subdivs[i % len(sorted_subdivs)][0]
                meters_by_subdiv[sub_div]['count'] += 1
        
        # Generate meters for each sub-division
        meter_id = 1
        for sub_div, target_info in meters_by_subdiv.items():
            sub_div_info = next((s for s in self.all_sub_divisions if s['sub_division'] == sub_div), None)
            if not sub_div_info:
                continue
            
            # Get transformers in this sub-division
            subdiv_transformers = transformers_by_subdiv.get(sub_div, [])
            if not subdiv_transformers:
                continue
            
            # Calculate how many meters per transformer
            meters_per_transformer = target_info['count'] // len(subdiv_transformers)
            extra_meters = target_info['count'] % len(subdiv_transformers)
            
            for idx, transformer_row in enumerate(subdiv_transformers):
                # Convert Series to dict for easier manipulation
                transformer = transformer_row.to_dict() if hasattr(transformer_row, 'to_dict') else transformer_row
                
                # This transformer gets base + maybe extra
                trans_meters = meters_per_transformer + (1 if idx < extra_meters else 0)
                
                for _ in range(trans_meters):
                    # Determine tariff based on consumer mix for this sub-division
                    consumer_mix = sub_div_info['consumer_mix']
                    
                    # Select tariff category based on mix
                    tariff = self._select_tariff_by_mix(consumer_mix, transformer['phase_type'])
                    
                    # Connection date (some old, some recent)
                    connection_date = fake.date_between(
                        start_date=pd.to_datetime(current_date) - timedelta(days=8*365),
                        end_date=current_date
                    )
                    
                    consumer_id = f"CI{random.randint(1000000, 9999999)}"
                    meter_number = f"{random.randint(10000000000, 99999999999)}"
                    
                    # Generate address with local flavor
                    address = self._generate_region_address(
                        sub_div_info['district'],
                        sub_div_info['division'],
                        sub_div
                    )
                    
                    # Determine if consumer has solar (higher in newer developments)
                    has_solar = random.random() > (0.9 if connection_date.year < 2020 else 0.85)
                    
                    meter = {
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
                        'address': address,
                        'district': sub_div_info['district'],
                        'division': sub_div_info['division'],
                        'sub_division': sub_div,
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
                        'feeder_name': transformer['feeder_name'],
                        'grid_transformer_id': transformer['grid_transformer_id'],
                        'distribution_transformer_id': transformer['transformer_id'],
                        'phase_type': transformer['phase_type'],
                        'meter_type': random.choices(['Smart', 'Smart', 'Conventional'], 
                                                    weights=[0.7, 0.7, 0.3])[0],
                        'meter_make': random.choice(['Landis+Gyr', 'Siemens', 'Itron', 'Elster']),
                        'meter_model': random.choice(['EM1200', 'SGM3000', 'AX-03', 'PM-500']),
                        'latitude': transformer['latitude'] + random.uniform(-0.002, 0.002),
                        'longitude': transformer['longitude'] + random.uniform(-0.002, 0.002),
                        'status': 'Active',
                        'has_solar': has_solar,
                        'solar_capacity_kw': round(random.uniform(1, 10), 2) if has_solar else 0,
                        'solar_installation_date': fake.date_between(
                            start_date=max(connection_date, pd.to_datetime('2018-01-01')),
                            end_date=current_date
                        ) if has_solar else None,
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
        
        # Trim to exact number if needed
        if len(meters) > num_meters:
            meters = random.sample(meters, num_meters)
        
        print(f"\n✅ Generated {len(meters)} meters across {len(meters_by_subdiv)} sub-divisions")
        
        # Print distribution by district
        district_counts = defaultdict(int)
        for m in meters:
            district_counts[m['district']] += 1
        
        print("\n📊 Meter Distribution by District:")
        for district, count in district_counts.items():
            pct = (count / len(meters)) * 100
            print(f"   - {district}: {count:,} ({pct:.1f}%)")
        
        return pd.DataFrame(meters)

    def _select_tariff_by_mix(self, consumer_mix: Dict, phase_type: str) -> str:
        """Select tariff based on sub-division consumer mix"""
        
        # Determine category based on mix
        categories = list(consumer_mix.keys())
        weights = list(consumer_mix.values())
        
        category = random.choices(categories, weights=weights, k=1)[0]
        
        # Map category to specific tariff
        if category == 'residential':
            return 'A-1a' if phase_type == '1-phase' else 'A-1b'
        elif category == 'commercial':
            return 'A-2a' if phase_type == '1-phase' else 'A-2b'
        elif category == 'industrial':
            # Small vs large industrial
            return random.choice(['B-1', 'B-2']) if phase_type == '3-phase' else 'B-1'
        elif category == 'agricultural':
            return 'C-1'
        else:  # bulk
            return 'D-1'

    def _generate_region_address(self, district: str, division: str, sub_division: str) -> str:
        """Generate realistic address for specific region"""
        
        # Locality names by district
        localities = {
            'ISLAMABAD': ['G-7/4', 'G-8/1', 'G-9/4', 'F-7/3', 'F-8/1', 'F-10/2', 'I-8/2', 'I-10/3'],
            'RAWALPINDI': ['Committee Chowk', 'Sadiqabad', 'Tench Bhatta', 'Chah Sultan', 'Dhoke Mangtal'],
            'ATTOCK': ['Civil Quarters', 'Kamra Road', 'Nala Mohra', 'Shahdand'],
            'JHELUM': ['Civil Lines', 'G.T. Road', 'Kachehri Chowk', 'Mandi City'],
            'CHAKWAL': ['Mohalla Lathi', 'Ratta Mohra', 'Shamsabad']
        }
        
        # Street types
        street_types = ['Street', 'Road', 'Boulevard', 'Lane', 'Avenue', 'Chowk']
        
        if district in localities:
            locality = random.choice(localities[district])
        else:
            locality = f"Main Bazar"
        
        house_no = random.randint(1, 500)
        street = f"{random.choice(street_types)} {random.randint(1, 30)}"
        
        return f"H.No. {house_no}, {street}, {locality}, {sub_division}, {division}, {district}"

    def simulate_monthly_events_region(self,
                                      meters_df: pd.DataFrame,
                                      transformers_df: pd.DataFrame,
                                      current_date: str,
                                      month_index: int) -> Tuple[pd.DataFrame, pd.DataFrame, List]:
        """
        Simulate monthly events with regional variations
        """
        
        current_date = pd.to_datetime(current_date)
        events = []
        
        meters = meters_df.to_dict('records') if isinstance(meters_df, pd.DataFrame) else meters_df
        transformers = transformers_df.to_dict('records') if isinstance(transformers_df, pd.DataFrame) else transformers_df
        
        # 1. NEW CONNECTIONS - varies by district growth rate
        for district_name, district_info in self.districts.items():
            district_meters = [m for m in meters if m.get('district') == district_name and m['is_active']]
            
            # Monthly growth rate (annual/12)
            monthly_growth = district_info['growth_rate'] / 12
            new_connections = np.random.poisson(max(1, int(len(district_meters) * monthly_growth)))
            
            # Find transformers in this district with capacity
            district_transformers = [t for t in transformers 
                                   if t.get('district') == district_name 
                                   and t['transformer_type'] == 'distribution'
                                   and t['capacity_utilization_pct'] < 85]
            
            if district_transformers and new_connections > 0:
                # Select transformers based on available capacity
                selected_transformers = random.choices(
                    district_transformers,
                    weights=[max(10, 100 - t['capacity_utilization_pct']) for t in district_transformers],
                    k=min(new_connections, len(district_transformers))
                )
                
                for transformer in selected_transformers:
                    # Generate new meter
                    sub_div_info = next((s for s in self.all_sub_divisions 
                                       if s['sub_division'] == transformer['sub_division']), None)
                    
                    if sub_div_info:
                        new_meter = self._generate_new_meter_region(transformer, sub_div_info, current_date, meters)
                        meters.append(new_meter)
                        
                        # Update transformer
                        transformer['consumers_connected'] = transformer.get('consumers_connected', 0) + 1
                        transformer['capacity_utilization_pct'] = min(95,
                            transformer['capacity_utilization_pct'] + random.uniform(0.5, 2))
                        
                        events.append({
                            'date': current_date,
                            'event_type': 'new_connection',
                            'district': district_name,
                            'sub_division': transformer['sub_division'],
                            'meter_number': new_meter['meter_number'],
                            'consumer_id': new_meter['consumer_id'],
                            'transformer_id': transformer['transformer_id']
                        })
        
        # 2. METER REPLACEMENTS (based on meter age)
        active_meters = [m for m in meters if m['is_active']]
        for meter in active_meters:
            # Older meters more likely to fail
            install_date = pd.to_datetime(meter['installation_date'])
            meter_age_days = (current_date - install_date).days
            failure_prob = meter_age_days / (10 * 365) * 0.01  # 1% after 10 years
            
            if random.random() < failure_prob:
                # Replace this meter
                new_meter = self._replace_meter_region(meter, current_date)
                meters.append(new_meter)
                
                meter['is_active'] = False
                meter['deactivation_date'] = current_date
                meter['status'] = 'Replaced'
                
                events.append({
                    'date': current_date,
                    'event_type': 'meter_replacement',
                    'district': meter['district'],
                    'old_meter': meter['meter_number'],
                    'new_meter': new_meter['meter_number'],
                    'consumer_id': meter['consumer_id'],
                    'reason': 'End of life' if meter_age_days > 8*365 else 'Fault'
                })
        
        # 3. TRANSFORMER UPGRADES (when utilization high)
        for transformer in [t for t in transformers if t['transformer_type'] == 'distribution']:
            if transformer['capacity_utilization_pct'] > 85 and random.random() < 0.02:
                # Upgrade this transformer
                old_rating = transformer['rating_kva']
                
                # Find upgrade path
                for spec in self.transformer_specs['distribution_transformer']['types']:
                    if spec['rating_kva'] == old_rating:
                        new_rating = spec['upgrade_to']
                        break
                else:
                    new_rating = old_rating * 1.5
                
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
                    'district': transformer['district'],
                    'transformer_id': transformer['transformer_id'],
                    'old_rating_kva': old_rating,
                    'new_rating_kva': new_rating
                })
        
        # Convert back to DataFrames
        meters_df = pd.DataFrame(meters)
        transformers_df = pd.DataFrame(transformers)
        
        return meters_df, transformers_df, events

    def _generate_new_meter_region(self, transformer: Dict, sub_div_info: Dict, 
                                  connection_date: datetime, existing_meters: List) -> Dict:
        """Generate new meter for specific region"""
        
        # Select tariff based on sub-division mix
        tariff = self._select_tariff_by_mix(sub_div_info['consumer_mix'], transformer['phase_type'])
        
        # Generate unique meter number
        while True:
            meter_number = f"{random.randint(10000000000, 99999999999)}"
            if not any(m['meter_number'] == meter_number for m in existing_meters):
                break
        
        consumer_id = f"CI{random.randint(1000000, 9999999)}"
        
        address = self._generate_region_address(
            sub_div_info['district'],
            sub_div_info['division'],
            sub_div_info['sub_division']
        )
        
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
            'address': address,
            'district': sub_div_info['district'],
            'division': sub_div_info['division'],
            'sub_division': sub_div_info['sub_division'],
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
            'feeder_name': transformer['feeder_name'],
            'grid_transformer_id': transformer['grid_transformer_id'],
            'distribution_transformer_id': transformer['transformer_id'],
            'phase_type': transformer['phase_type'],
            'meter_type': 'Smart',  # New meters are smart
            'meter_make': random.choice(['Landis+Gyr', 'Siemens', 'Itron']),
            'meter_model': random.choice(['EM1200', 'SGM3000', 'AX-03']),
            'latitude': transformer['latitude'] + random.uniform(-0.001, 0.001),
            'longitude': transformer['longitude'] + random.uniform(-0.001, 0.001),
            'status': 'Active',
            'has_solar': random.random() > 0.9,
            'solar_capacity_kw': round(random.uniform(1, 5), 2) if random.random() > 0.9 else 0,
            'solar_installation_date': connection_date if random.random() > 0.9 else None,
            'average_monthly_consumption': 0,
            'billing_status': 'Regular',
            'payment_method': random.choice(['JazzCash', 'EasyPaisa', 'Online']),
            'email': fake.email(),
            'lifecycle_events': []
        }

    def _replace_meter_region(self, old_meter: Dict, replacement_date: datetime) -> Dict:
        """Create replacement meter"""
        
        new_meter = old_meter.copy()
        new_meter['meter_number'] = f"{random.randint(10000000000, 99999999999)}"
        new_meter['previous_meter_number'] = old_meter['meter_number']
        new_meter['meter_generation'] = old_meter.get('meter_generation', 1) + 1
        new_meter['installation_date'] = replacement_date
        new_meter['deactivation_date'] = None
        new_meter['is_active'] = True
        new_meter['status'] = 'Active'
        new_meter['meter_type'] = 'Smart'  # Upgrade to smart
        new_meter['lifecycle_events'] = old_meter.get('lifecycle_events', []) + [{
            'date': replacement_date,
            'event': 'meter_replacement',
            'old_meter': old_meter['meter_number']
        }]
        
        return new_meter

    def generate_region_readings(self,
                                meters_df: pd.DataFrame,
                                transformers_df: pd.DataFrame,
                                events_log: List,
                                start_date: str,
                                end_date: str,
                                frequency_minutes: int = 15) -> pd.DataFrame:
        """
        Generate readings with regional consumption patterns
        """
        
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        
        all_timestamps = pd.date_range(start=start, end=end, freq=f'{frequency_minutes}min')
        
        readings = []
        
        # Create failure periods lookup
        failures_by_meter = defaultdict(list)
        for event in events_log:
            if event.get('event_type') == 'meter_failure':
                failure_start = event['date']
                failure_end = failure_start + timedelta(days=event.get('failure_duration_days', 1))
                failures_by_meter[event['meter_number']].append((failure_start, failure_end))
        
        # Group by district for different consumption patterns
        meters_by_district = meters_df.groupby('district')
        
        print("\n📈 Generating readings by district...")
        
        for district, district_meters in meters_by_district:
            print(f"   Processing {district} ({len(district_meters)} meters)")
            
            for _, meter in tqdm(district_meters.iterrows(), desc=district, leave=False):
                meter_number = meter['meter_number']
                consumer_id = meter['consumer_id']
                tariff = meter['tariff_category']
                transformer_id = meter['distribution_transformer_id']
                district_name = meter['district']
                
                installation_date = pd.to_datetime(meter['installation_date'])
                deactivation_date = pd.to_datetime(meter['deactivation_date']) if pd.notna(meter['deactivation_date']) else None
                
                # Get failures for this meter
                meter_failures = failures_by_meter.get(meter_number, [])
                
                # Consumption parameters based on tariff and district
                base_min, base_max, peak_min, peak_max = self._get_consumption_params(tariff)
                
                # District-specific adjustments
                district_multiplier = {
                    'ISLAMABAD': 1.3,  # Higher consumption in capital
                    'RAWALPINDI': 1.2,
                    'ATTOCK': 1.0,
                    'JHELUM': 0.9,
                    'CHAKWAL': 0.8
                }.get(district_name, 1.0)
                
                cumulative_reading = 0
                previous_reading = 0
                
                for timestamp in all_timestamps:
                    # Check if meter was active
                    if timestamp < installation_date:
                        continue
                    if deactivation_date and timestamp > deactivation_date:
                        continue
                    
                    # Check if in failure period
                    in_failure = any(start <= timestamp <= end for start, end in meter_failures)
                    if in_failure:
                        continue
                    
                    hour = timestamp.hour
                    month = timestamp.month
                    day = timestamp.dayofweek
                    
                    # Peak hours (regional variations)
                    if district_name in ['ISLAMABAD', 'RAWALPINDI']:
                        # Urban areas have longer peak
                        is_peak = (5 <= hour <= 10) or (17 <= hour <= 23)
                    else:
                        # Rural areas have earlier evening peak
                        is_peak = (5 <= hour <= 9) or (18 <= hour <= 22)
                    
                    # Weekend adjustment
                    weekend_multiplier = 1.3 if day >= 5 else 1.0
                    
                    # Seasonal adjustment
                    summer_months = [5, 6, 7, 8, 9]
                    seasonal_multiplier = 1.5 if month in summer_months else 1.0
                    
                    # Base consumption
                    if is_peak:
                        base_consumption = random.uniform(peak_min, peak_max)
                    else:
                        base_consumption = random.uniform(base_min, base_max)
                    
                    # Apply multipliers
                    consumption = base_consumption * weekend_multiplier * seasonal_multiplier * district_multiplier
                    
                    # Solar impact
                    if meter['has_solar'] and 8 <= hour <= 17:
                        solar_factor = random.uniform(0.3, 0.7)
                        consumption *= solar_factor
                    
                    # Random variation
                    consumption *= random.uniform(0.9, 1.1)
                    
                    cumulative_reading += consumption
                    
                    # Generate electrical parameters
                    voltage = self._generate_voltage_region(timestamp, district_name)
                    current = (consumption * 1000) / voltage if voltage > 0 else 0
                    frequency = 50.0 + random.gauss(0, 0.1)
                    power_factor = 0.92 + random.gauss(0, 0.02)
                    temperature = self._generate_temperature_region(timestamp, district_name)
                    signal_strength = -70 + random.gauss(0, 5)
                    battery_voltage = 3.7 + random.gauss(0, 0.1)
                    
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
                        'district': district_name,
                        'sub_division': meter['sub_division'],
                        'distribution_transformer_id': transformer_id,
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
                        'solar_active': meter['has_solar'] and 8 <= hour <= 17,
                        'is_peak_hour': is_peak
                    }
                    
                    readings.append(reading)
                    previous_reading = cumulative_reading
        
        return pd.DataFrame(readings)

    def _generate_voltage_region(self, timestamp, district: str) -> float:
        """Generate voltage with regional characteristics"""
        hour = timestamp.hour
        
        # Urban areas have more stable voltage
        if district in ['ISLAMABAD', 'RAWALPINDI']:
            base = 230
            variation = 3
        else:
            base = 225
            variation = 5
        
        # Peak hour drop
        if 18 <= hour <= 22:
            base *= 0.97
        
        return base + random.gauss(0, variation)

    def _generate_temperature_region(self, timestamp, district: str) -> float:
        """Generate temperature with regional variation"""
        hour = timestamp.hour
        month = timestamp.month
        
        # Base temperature by district
        base_temps = {
            'ISLAMABAD': {'summer': 35, 'winter': 8, 'spring': 25},
            'RAWALPINDI': {'summer': 36, 'winter': 7, 'spring': 26},
            'ATTOCK': {'summer': 38, 'winter': 6, 'spring': 24},
            'JHELUM': {'summer': 40, 'winter': 5, 'spring': 27},
            'CHAKWAL': {'summer': 39, 'winter': 4, 'spring': 23}
        }
        
        if month in [5, 6, 7, 8]:
            base = base_temps[district]['summer']
        elif month in [12, 1, 2]:
            base = base_temps[district]['winter']
        else:
            base = base_temps[district]['spring']
        
        # Daily variation
        if 14 <= hour <= 16:
            base += 5
        elif 4 <= hour <= 6:
            base -= 5
        
        return base + random.gauss(0, 2)

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
                    return consumption, cumulative_reading, voltage, frequency, signal_strength, 2.8, battery_voltage, 'Battery Fault'
                elif issue == 'meter_tamper':
                    return consumption * 0.3, previous_reading + consumption * 0.3, voltage, frequency, signal_strength, battery_voltage, 'Meter Tamper'
                elif issue == 'reverse_energy':
                    return -consumption, previous_reading - consumption, voltage, frequency, signal_strength, battery_voltage, 'Reverse Energy'
        
        return consumption, cumulative_reading, voltage, frequency, signal_strength, battery_voltage, 'Normal'

    def _get_consumption_params(self, tariff: str) -> Tuple[float, float, float, float]:
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

    def generate_region_bills(self,
                            meters_df: pd.DataFrame,
                            readings_df: pd.DataFrame,
                            start_date: str,
                            end_date: str) -> pd.DataFrame:
        """Generate monthly bills with regional variations"""
        
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        
        billing_months = pd.date_range(start=start, end=end, freq='MS')
        bills = []
        
        print("\n💰 Generating monthly bills...")
        
        for meter_number, meter_readings in tqdm(readings_df.groupby('meter_number'), desc="Processing meters"):
            meter_info = meters_df[meters_df['meter_number'] == meter_number].iloc[0]
            
            meter_readings['timestamp'] = pd.to_datetime(meter_readings['timestamp'])
            
            for billing_month in billing_months:
                month_str = billing_month.strftime('%b %y').upper()
                next_month = billing_month + pd.DateOffset(months=1)
                
                month_readings = meter_readings[
                    (meter_readings['timestamp'] >= billing_month) &
                    (meter_readings['timestamp'] < next_month)
                ]
                
                if len(month_readings) == 0:
                    continue
                
                # Calculate consumption
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
                bill = self._calculate_bill_region(
                    consumption,
                    meter_info['tariff_category'],
                    meter_info['connected_load_kw'],
                    month_str,
                    meter_number,
                    meter_info['consumer_id'],
                    meter_info['name'],
                    meter_info['address'],
                    meter_info['district'],
                    meter_info['distribution_transformer_id']
                )
                
                bill['district'] = meter_info['district']
                bill['sub_division'] = meter_info['sub_division']
                bill['meter_generation'] = meter_info['meter_generation']
                bill['has_solar'] = meter_info['has_solar']
                
                bills.append(bill)
        
        return pd.DataFrame(bills)

    def _calculate_bill_region(self, consumption_kwh, tariff, connected_load,
                              billing_month, meter_number, consumer_id,
                              consumer_name, address, district, transformer_id):
        """Calculate electricity bill with regional adjustments"""
        
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
            'district': district,
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

    def save_region_data(self, meters_df, transformers_df, readings_df, bills_df, events_df, output_dir):
        """Save all data with region-based organization"""
        
        os.makedirs(output_dir, exist_ok=True)
        
        # Save main datasets
        transformers_df.to_csv(os.path.join(output_dir, 'transformers_complete.csv'), index=False)
        meters_df.to_csv(os.path.join(output_dir, 'meters_complete.csv'), index=False)
        readings_df.to_csv(os.path.join(output_dir, 'readings_summary.csv'), index=False)
        bills_df.to_csv(os.path.join(output_dir, 'bills_complete.csv'), index=False)
        events_df.to_csv(os.path.join(output_dir, 'lifecycle_events.csv'), index=False)
        
        # Save district-wise summaries
        for district in meters_df['district'].unique():
            district_dir = os.path.join(output_dir, 'by_district', district.replace(' ', '_'))
            os.makedirs(district_dir, exist_ok=True)
            
            # District meters
            district_meters = meters_df[meters_df['district'] == district]
            district_meters.to_csv(os.path.join(district_dir, 'meters.csv'), index=False)
            
            # District transformers
            district_transformers = transformers_df[transformers_df['district'] == district]
            district_transformers.to_csv(os.path.join(district_dir, 'transformers.csv'), index=False)
            
            # District bills
            district_bills = bills_df[bills_df['district'] == district]
            district_bills.to_csv(os.path.join(district_dir, 'bills.csv'), index=False)
            
            print(f"   Saved {district} data ({len(district_meters)} meters)")

    def generate_all_data(self,
                         initial_meters: int = 10000,
                         start_date: str = '2023-01-01',
                         end_date: str = '2025-01-31',
                         reading_frequency: int = 15,
                         output_dir: str = './iesco_complete_data'):
        """
        Main method to generate complete IESCO dataset covering all regions
        """
        
        print("="*80)
        print("IESCO COMPLETE REGIONAL DATA GENERATOR")
        print("="*80)
        print(f"Coverage: 5 Districts, {len(self.all_sub_divisions)} Sub-Divisions")
        print("="*80)
        
        # Step 1: Generate transformers across all regions
        print("\n📊 STEP 1: Generating Transformer Infrastructure")
        transformers_df = self.generate_region_transformers(target_dist_transformers=2500)
        
        # Step 2: Generate initial meters
        print("\n👥 STEP 2: Generating Initial Meters")
        meters_df = self.generate_region_meters(initial_meters, transformers_df, start_date)
        
        # Step 3: Simulate monthly events
        print("\n🔄 STEP 3: Simulating Monthly Events")
        all_events = []
        current_meters = meters_df.copy()
        current_transformers = transformers_df.copy()
        
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        months = pd.date_range(start=start, end=end, freq='MS')
        
        for month_idx, month_start in enumerate(tqdm(months, desc="Processing months")):
            current_meters, current_transformers, month_events = self.simulate_monthly_events_region(
                current_meters, current_transformers, month_start, month_idx
            )
            all_events.extend(month_events)
        
        events_df = pd.DataFrame(all_events)
        
        print(f"\n✅ Events Simulated: {len(all_events)}")
        print(f"   - Final Active Meters: {len(current_meters[current_meters['is_active']])}")
        
        # Step 4: Generate readings
        print("\n📈 STEP 4: Generating Meter Readings")
        readings_df = self.generate_region_readings(
            current_meters, current_transformers, all_events,
            start_date, end_date, reading_frequency
        )
        
        # Step 5: Generate bills
        print("\n💰 STEP 5: Generating Monthly Bills")
        bills_df = self.generate_region_bills(
            current_meters, readings_df, start_date, end_date
        )
        
        # Step 6: Save all data
        print("\n💾 STEP 6: Saving Data")
        self.save_region_data(current_meters, current_transformers, 
                              readings_df, bills_df, events_df, output_dir)
        
        # Print final summary
        self.print_region_summary(current_meters, readings_df, bills_df, 
                                   current_transformers, events_df, output_dir)
        
        return {
            'meters': current_meters,
            'transformers': current_transformers,
            'readings': readings_df,
            'bills': bills_df,
            'events': events_df
        }

    def print_region_summary(self, meters_df, readings_df, bills_df, transformers_df, events_df, output_dir):
        """Print comprehensive regional summary"""
        
        print("\n" + "="*80)
        print("✅ GENERATION COMPLETE - REGIONAL SUMMARY")
        print("="*80)
        
        print("\n📁 OUTPUT STRUCTURE:")
        print(f"   {output_dir}/")
        print(f"   ├── transformers_complete.csv")
        print(f"   ├── meters_complete.csv")
        print(f"   ├── readings_summary.csv")
        print(f"   ├── bills_complete.csv")
        print(f"   ├── lifecycle_events.csv")
        print(f"   └── by_district/")
        print(f"       ├── ISLAMABAD/")
        print(f"       ├── RAWALPINDI/")
        print(f"       ├── ATTOCK/")
        print(f"       ├── JHELUM/")
        print(f"       └── CHAKWAL/")
        
        print("\n📊 COVERAGE STATISTICS:")
        print(f"   Districts: {meters_df['district'].nunique()}")
        print(f"   Divisions: {meters_df['division'].nunique()}")
        print(f"   Sub-Divisions: {meters_df['sub_division'].nunique()}")
        
        print("\n📊 INFRASTRUCTURE:")
        print(f"   Grid Transformers: {len(transformers_df[transformers_df['transformer_type'] == 'grid'])}")
        print(f"   Distribution Transformers: {len(transformers_df[transformers_df['transformer_type'] == 'distribution'])}")
        
        print("\n📊 METERS BY DISTRICT:")
        for district in sorted(meters_df['district'].unique()):
            district_meters = meters_df[meters_df['district'] == district]
            active = len(district_meters[district_meters['is_active']])
            total = len(district_meters)
            pct = (active / total) * 100 if total > 0 else 0
            print(f"   - {district}: {active:,} active / {total:,} total ({pct:.1f}%)")
        
        print("\n📊 METERS BY CATEGORY:")
        for category in meters_df['consumer_category'].unique():
            count = len(meters_df[meters_df['consumer_category'] == category])
            pct = (count / len(meters_df)) * 100
            print(f"   - {category.title()}: {count:,} ({pct:.1f}%)")
        
        print("\n📊 READINGS:")
        print(f"   Total Readings: {len(readings_df):,}")
        print(f"   Date Range: {readings_df['timestamp'].min()} to {readings_df['timestamp'].max()}")
        print(f"   Total Energy: {readings_df['energy_consumed_kwh'].sum():,.0f} kWh")
        
        data_quality = readings_df['data_quality_flag'].value_counts()
        print(f"\n📊 DATA QUALITY:")
        for flag, count in data_quality.head(5).items():
            pct = (count / len(readings_df)) * 100
            print(f"   - {flag}: {pct:.2f}%")
        
        print("\n📊 BILLING:")
        print(f"   Total Bills: {len(bills_df):,}")
        print(f"   Total Amount: Rs. {bills_df['total_amount'].sum():,.2f}")
        print(f"   Average Bill: Rs. {bills_df['total_amount'].mean():,.2f}")
        
        print("\n📊 LIFECYCLE EVENTS:")
        if len(events_df) > 0:
            for event_type in events_df['event_type'].value_counts().head(5).items():
                print(f"   - {event_type[0]}: {event_type[1]}")
        
        print("\n" + "="*80)
        print(f"✅ All data saved to: {output_dir}")
        print("="*80)


def main():
    parser = argparse.ArgumentParser(description='Generate Complete IESCO Regional Data')
    parser.add_argument('--initial_meters', type=int, default=10000,
                       help='Initial number of meters (default: 10000)')
    parser.add_argument('--start_date', type=str, default='2023-01-01',
                       help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end_date', type=str, default='2025-01-31',
                       help='End date (YYYY-MM-DD)')
    parser.add_argument('--frequency', type=int, default=15,
                       help='Reading frequency in minutes')
    parser.add_argument('--output_dir', type=str, default='./iesco_complete_data',
                       help='Output directory')
    
    args = parser.parse_args()
    
    generator = IESCOCompleteRegionGenerator()
    
    data = generator.generate_all_data(
        initial_meters=args.initial_meters,
        start_date=args.start_date,
        end_date=args.end_date,
        reading_frequency=args.frequency,
        output_dir=args.output_dir
    )


if __name__ == "__main__":
    main()