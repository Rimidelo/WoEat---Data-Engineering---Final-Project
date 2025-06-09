#!/usr/bin/env python3
"""
WoEat Data Engineering Final Project - Interactive Demo Runner
Run this script for a smooth, guided demonstration
"""

import subprocess
import time
import sys

def print_banner(text, char="="):
    """Print a formatted banner"""
    print(f"\n{char * 60}")
    print(f"🎯 {text}")
    print(f"{char * 60}\n")

def wait_for_user():
    """Wait for user to press Enter"""
    input("👉 Press Enter to continue...")

def run_command(cmd, description):
    """Run a Docker command with nice formatting"""
    print(f"🚀 {description}")
    print(f"💻 Running: {cmd}")
    print("-" * 40)
    
    try:
        result = subprocess.run(cmd, shell=True, capture_output=False, text=True)
        if result.returncode == 0:
            print(f"✅ {description} - COMPLETED")
        else:
            print(f"❌ {description} - FAILED")
    except Exception as e:
        print(f"❌ Error: {e}")
    
    print("-" * 40)

def main():
    print_banner("WoEat Data Engineering Final Project - Live Demo", "🎯")
    
    print("""
    📋 This demo will showcase:
    
    1. 🥉 Bronze Layer - Raw data ingestion
    2. 🥈 Silver Layer - Data quality & cleaning  
    3. 🥇 Gold Layer - Business analytics with SCD Type 2
    4. 🔄 Late-Arriving Data - Real-world scenario handling
    
    ⏱️  Total demo time: ~5 minutes
    """)
    
    wait_for_user()
    
    # Step 1: Bronze Layer
    print_banner("Step 1: Bronze Layer - Data Ingestion")
    print("""
    🎙️ NARRATION:
    "This ingests raw data from multiple sources - orders, drivers, restaurants, 
    weather data. Notice how we handle different data formats and sources, creating 
    a unified Bronze layer in Iceberg format with full schema evolution support."
    """)
    wait_for_user()
    
    run_command(
        "docker exec spark-iceberg spark-submit /tmp/bronze_simple.py",
        "Bronze Layer Data Ingestion"
    )
    
    print("""
    📊 Expected Results:
    ✅ 13 menu items
    ✅ 20 drivers  
    ✅ 10 restaurant performance records
    ✅ 8 weather records
    ✅ 3 sample orders
    """)
    wait_for_user()
    
    # Step 2: Silver Layer
    print_banner("Step 2: Silver Layer - Data Quality & Cleaning")
    print("""
    🎙️ NARRATION:
    "The Silver layer applies data quality rules, validates business constraints, 
    and performs data cleansing. Notice the comprehensive quality reports showing 
    100% pass rates for our validation rules."
    """)
    wait_for_user()
    
    run_command(
        "docker exec spark-iceberg spark-submit /tmp/silver_processing.py",
        "Silver Layer Data Quality Processing"
    )
    
    print("""
    📊 Expected Results:
    ✅ Data Quality Checks: 100% Pass Rate
    ✅ Null Validation: Passed
    ✅ Business Rules: Validated
    """)
    wait_for_user()
    
    # Step 3: Gold Layer
    print_banner("Step 3: Gold Layer - Business Analytics with SCD Type 2")
    print("""
    🎙️ NARRATION:
    "The Gold layer implements a star schema with fact tables and slowly changing 
    dimensions. Watch how SCD Type 2 tracks historical changes in driver information 
    with effective dates and current flags."
    """)
    wait_for_user()
    
    run_command(
        "docker exec spark-iceberg spark-submit /tmp/gold_processing.py",
        "Gold Layer Dimensional Modeling"
    )
    
    print("""
    📊 Expected Results:
    ✅ Star Schema: Created
    ✅ SCD Type 2: Active
    ✅ Business Metrics: Generated
    ✅ Historical Tracking: Enabled
    """)
    wait_for_user()
    
    # Step 4: Late-Arriving Data (MAIN FEATURE)
    print_banner("Step 4: 🔥 MAIN FEATURE - Late-Arriving Data Demo", "🔥")
    print("""
    🎙️ NARRATION:
    "Now for the key feature - handling late-arriving data. In real food delivery 
    platforms, restaurant performance reports often arrive at the end of the day, 
    sometimes 24-48 hours late. Let me show you how our system handles this."
    
    📊 What to Watch For:
    1. Before State: Original restaurant data with on-time arrivals
    2. Late Data Arrival: Restaurant reports for June 4th-5th arriving on June 7th
    3. Impact Analysis: CRITICAL and WARNING alerts
    4. Automatic Handling: System detects and flags late arrivals
    5. Reprocessing: Triggers updates to Silver/Gold layers
    """)
    wait_for_user()
    
    run_command(
        "docker exec spark-iceberg spark-submit /tmp/late_arriving_data.py",
        "🔥 Late-Arriving Data Demonstration"
    )
    
    print("""
    📊 Expected Results:
    🕐 Late Records Detected: 10
    📅 Affected Dates: 2
    🚨 Critical Alerts: 5 (3+ days late)
    ⚠️ Warning Alerts: 5 (2+ days late)
    ✅ Reprocessing: Triggered
    """)
    
    # Demo Complete
    print_banner("🎉 Demo Complete - Key Takeaways")
    print("""
    🎯 What We Demonstrated:
    
    ✅ Complete Data Lakehouse Architecture (Bronze → Silver → Gold)
    ✅ Real-world Problem Solving (Late-arriving restaurant data)
    ✅ Production-ready Data Engineering (Quality, Monitoring, Alerting)
    ✅ Modern Technology Stack (Spark, Iceberg, Kafka, Airflow, Docker)
    ✅ Advanced Features (SCD Type 2, Event-time processing, Watermarks)
    
    🏆 This showcases industry-standard data engineering practices
       solving real business problems with robust, scalable solutions.
    
    ❓ Ready for questions!
    """)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n👋 Demo interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Demo error: {e}")
        sys.exit(1) 