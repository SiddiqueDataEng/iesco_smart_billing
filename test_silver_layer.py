"""
Quick test for Silver Layer processing
Verifies Spark setup and runs a small test
"""

print("="*80)
print("SILVER LAYER - QUICK TEST")
print("="*80)

# Test 1: Check if PySpark is installed
print("\n1. Checking PySpark installation...")
try:
    import pyspark
    print(f"   ✅ PySpark installed: version {pyspark.__version__}")
except ImportError:
    print("   ❌ PySpark not installed")
    print("   Run: pip install pyspark")
    exit(1)

# Test 2: Check if Spark home exists
print("\n2. Checking Spark installation...")
import os
spark_home = "C:\\spark4"
if os.path.exists(spark_home):
    print(f"   ✅ Spark found at: {spark_home}")
else:
    print(f"   ⚠️  Spark not found at: {spark_home}")
    print("   Silver layer will still work with PySpark package")

# Test 3: Check if Bronze data exists
print("\n3. Checking Bronze Layer data...")
bronze_path = "./iesco_complete_data"
required_files = ['meters.csv', 'readings.csv', 'bills.csv', 'payments.csv']
missing_files = []

for file in required_files:
    filepath = os.path.join(bronze_path, file)
    if os.path.exists(filepath):
        size_mb = os.path.getsize(filepath) / (1024 * 1024)
        print(f"   ✅ {file}: {size_mb:.2f} MB")
    else:
        print(f"   ❌ {file}: NOT FOUND")
        missing_files.append(file)

if missing_files:
    print(f"\n   ⚠️  Missing files: {', '.join(missing_files)}")
    print("   Run data generator first to create Bronze Layer")
    exit(1)

# Test 4: Try to create Spark session
print("\n4. Testing Spark session creation...")
try:
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .appName("Silver_Layer_Test") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    
    print(f"   ✅ Spark session created")
    print(f"      Version: {spark.version}")
    print(f"      Master: {spark.sparkContext.master}")
    
    # Test 5: Try to load a small sample
    print("\n5. Testing data load...")
    meters = spark.read.csv(f"{bronze_path}/meters.csv", header=True, inferSchema=True)
    meter_count = meters.count()
    print(f"   ✅ Loaded {meter_count:,} meters")
    
    # Show sample
    print("\n   Sample data:")
    meters.select('meter_number', 'consumer_type', 'sanctioned_load_kw').show(3, truncate=False)
    
    spark.stop()
    print("\n   ✅ Spark session stopped")
    
except Exception as e:
    print(f"   ❌ Error: {e}")
    exit(1)

# Test 6: Check if Jupyter is installed
print("\n6. Checking Jupyter installation...")
try:
    import jupyter
    print(f"   ✅ Jupyter installed")
except ImportError:
    print("   ⚠️  Jupyter not installed")
    print("   Run: pip install jupyter notebook")

print("\n" + "="*80)
print("✅ ALL TESTS PASSED")
print("="*80)
print("\nYou're ready to process Silver Layer!")
print("\nNext steps:")
print("  1. Run: start_jupyter_pyspark.bat")
print("  2. Or run: python silver_layer_processing.py")
print("="*80)
