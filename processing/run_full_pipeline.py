#!/usr/bin/env python3
"""
WoEat Data Engineering Pipeline - Complete Processing
Runs Bronze → Silver → Gold with new table structure
"""

import subprocess
import sys
import time
from pathlib import Path

def print_banner(text, char="="):
    """Print a formatted banner"""
    print(f"\n{char * 60}")
    print(f"🎯 {text}")
    print(f"{char * 60}\n")

def run_python_script(script_path, description):
    """Run a Python script and handle errors"""
    print(f"🚀 {description}")
    print(f"📂 Running: {script_path}")
    print("-" * 40)
    
    try:
        # Import and run the script's main functionality
        script_dir = Path(script_path).parent
        script_name = Path(script_path).stem
        
        # Add script directory to path
        sys.path.insert(0, str(script_dir))
        
        if script_name == "bronze_ingestion":
            from bronze_ingestion import BronzeIngestion
            processor = BronzeIngestion()
            processor.ingest_batch_data()
            processor.stop()
            
        elif script_name == "silver_processing":
            from silver_processing import SilverProcessing
            processor = SilverProcessing()
            processor.process_all_silver_tables()
            processor.run_data_quality_checks()
            processor.stop()
            
        elif script_name == "gold_processing":
            from gold_processing import GoldProcessing
            processor = GoldProcessing()
            processor.process_all_gold_tables()
            processor.stop()
            
        elif script_name == "generate_5000_orders":
            from generate_5000_orders import OrdersGenerator
            generator = OrdersGenerator()
            orders_count, items_count, ratings_count = generator.generate_5000_orders()
            print(f"Generated: {orders_count} orders, {items_count} items, {ratings_count} ratings")
            
        print(f"✅ {description} - COMPLETED")
        
    except Exception as e:
        print(f"❌ {description} - FAILED: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
    
    print("-" * 40)
    return True

def main():
    print_banner("WoEat Data Engineering Pipeline - Complete Run")
    
    print("""
    📋 This pipeline will process:
    
    1. 🥉 Bronze Layer - Raw data ingestion (batch)
    2. 🛍️  Generate 5000 orders - Large dataset
    3. 🥈 Silver Layer - Data quality & transformations
    4. 🥇 Gold Layer - Dimensional modeling & business metrics
    
    ⏱️  Estimated time: ~3-5 minutes
    """)
    
    input("👉 Press Enter to start pipeline...")
    
    # Step 1: Bronze Layer Batch Ingestion
    print_banner("Step 1: Bronze Layer - Raw Data Ingestion")
    success = run_python_script("bronze_ingestion.py", "Bronze Layer Batch Data Ingestion")
    if not success:
        print("❌ Pipeline failed at Bronze layer")
        return
    
    time.sleep(2)
    
    # Step 2: Generate Large Dataset
    print_banner("Step 2: Generate Large Dataset - 5000 Orders")
    success = run_python_script("generate_5000_orders.py", "Generate 5000 Orders with Items and Ratings")
    if not success:
        print("❌ Pipeline failed at data generation")
        return
        
    time.sleep(2)
    
    # Step 3: Silver Layer Processing
    print_banner("Step 3: Silver Layer - Data Quality & Cleaning")
    success = run_python_script("silver_processing.py", "Silver Layer Data Quality Processing")
    if not success:
        print("❌ Pipeline failed at Silver layer")
        return
        
    time.sleep(2)
    
    # Step 4: Gold Layer Processing
    print_banner("Step 4: Gold Layer - Dimensional Modeling")
    success = run_python_script("gold_processing.py", "Gold Layer Star Schema & Business Metrics")
    if not success:
        print("❌ Pipeline failed at Gold layer")
        return
    
    # Pipeline Complete
    print_banner("🎉 Pipeline Complete - Success!")
    print("""
    🎯 Processing Summary:
    
    ✅ Bronze Layer: Raw data ingested from multiple sources
       - Orders, Order Items, Ratings, Drivers, Restaurants, Menu Items, Weather
    
    ✅ Large Dataset: 5000 orders with realistic distribution
       - ~10,000+ order items
       - ~3,500+ ratings
       - Multi-restaurant, multi-driver coverage
    
    ✅ Silver Layer: Data quality and business logic applied
       - Calculated fields (prep time, delivery time, extended price)
       - Daily performance aggregations
       - Data validation and cleansing
    
    ✅ Gold Layer: Star schema for analytics
       - Fact tables: Orders, Order Items, Ratings, Daily Performance
       - Dimension tables: Drivers, Restaurants, Menu Items, Date
       - SCD Type 2 for historical tracking
       - Business summary metrics
    
    🏆 Your data lakehouse is ready for analytics!
    
    📊 Next Steps:
       - Connect BI tools to Gold layer
       - Set up real-time streaming
       - Configure monitoring and alerting
    """)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n👋 Pipeline interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Pipeline error: {e}")
        sys.exit(1) 