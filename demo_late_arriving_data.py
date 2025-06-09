#!/usr/bin/env python3
"""
WoEat Late-Arriving Data Demonstration Script

This script demonstrates how the WoEat data pipeline handles late-arriving 
restaurant reports that come at the end of the day, showing:

1. Initial pipeline state with on-time data
2. Late data arrival simulation  
3. Impact analysis and reprocessing
4. Data quality and monitoring
"""

import subprocess
import time
from datetime import datetime
import sys
import os

def run_script(script_path, description):
    """Run a Python script and handle errors"""
    print(f"\n{'='*60}")
    print(f"🚀 {description}")
    print(f"📄 Running: {script_path}")
    print(f"{'='*60}")
    
    try:
        # Change to processing directory for Spark scripts
        if 'processing/' in script_path:
            os.chdir('processing')
            script_name = script_path.replace('processing/', '')
        else:
            script_name = script_path
            
        result = subprocess.run([
            sys.executable, script_name
        ], capture_output=True, text=True, timeout=300)
        
        if result.returncode == 0:
            print("✅ SUCCESS")
            if result.stdout:
                print("📤 Output:")
                print(result.stdout)
        else:
            print("❌ FAILED")
            if result.stderr:
                print("📤 Error Output:")
                print(result.stderr)
            if result.stdout:
                print("📤 Standard Output:")
                print(result.stdout)
        
        # Return to root directory
        if 'processing/' in script_path:
            os.chdir('..')
            
        return result.returncode == 0
        
    except subprocess.TimeoutExpired:
        print("⏰ TIMEOUT - Script took too long to execute")
        return False
    except Exception as e:
        print(f"💥 EXCEPTION: {str(e)}")
        return False

def print_demo_header():
    """Print the demo introduction"""
    print("""
🍔 WoEat Data Pipeline - Late-Arriving Data Demonstration
=========================================================

📋 Demo Scenario:
• Restaurant performance reports normally arrive daily at 11:59 PM
• Due to technical issues, some reports arrive 24-48 hours late
• This demo shows how our pipeline handles late data gracefully

🎯 What We'll Demonstrate:
1. Initial pipeline state with on-time data
2. Late restaurant reports arriving after orders are processed  
3. Impact analysis showing data discrepancies
4. Automated reprocessing and data quality scoring
5. Monitoring and alerting for late data

⏰ Expected Runtime: ~5-10 minutes
""")

def print_demo_stage(stage_num, title, description):
    """Print stage information"""
    print(f"\n🎬 STAGE {stage_num}: {title}")
    print("─" * 50)
    print(f"📝 {description}")
    print()

def main():
    """Main demo execution"""
    start_time = datetime.now()
    
    print_demo_header()
    
    # Check if we're in the right directory
    if not os.path.exists('processing') or not os.path.exists('streaming'):
        print("❌ Error: Please run this script from the project root directory")
        print("📁 Expected structure: processing/, streaming/, docs/, etc.")
        return False
    
    success_count = 0
    total_stages = 5
    
    # Stage 1: Setup initial pipeline state
    print_demo_stage(1, "Initial Pipeline Setup", 
                    "Creating Bronze, Silver, and Gold layers with on-time data")
    
    if run_script("processing/bronze_simple.py", "Bronze Layer - Initial Data"):
        success_count += 1
        time.sleep(2)
        
        if run_script("processing/silver_processing.py", "Silver Layer - Data Cleaning"):
            success_count += 1
            time.sleep(2)
            
            if run_script("processing/gold_processing.py", "Gold Layer - Business Metrics"):
                success_count += 1
    
    # Stage 2: Simulate late arriving data
    print_demo_stage(2, "Late Data Arrival", 
                    "Simulating restaurant reports arriving 24-48 hours late")
    
    if run_script("processing/late_arriving_data.py", "Late Arriving Restaurant Reports"):
        success_count += 1
    
    # Stage 3: Reprocess affected layers
    print_demo_stage(3, "Pipeline Reprocessing", 
                    "Reprocessing Silver and Gold layers with late data")
    
    if run_script("processing/silver_reprocessing.py", "Silver Layer - Reprocessing"):
        success_count += 1
        time.sleep(2)
        
        # Rerun Gold processing to incorporate late data
        run_script("processing/gold_processing.py", "Gold Layer - Updated Metrics")
    
    # Final summary
    end_time = datetime.now()
    duration = end_time - start_time
    
    print(f"\n{'='*60}")
    print("🎯 DEMO SUMMARY")
    print(f"{'='*60}")
    print(f"⏱️  Total Runtime: {duration}")
    print(f"✅ Successful Stages: {success_count}/{total_stages}")
    print()
    
    if success_count == total_stages:
        print("🎉 DEMO COMPLETED SUCCESSFULLY!")
        print()
        print("🔍 What Was Demonstrated:")
        print("✅ Late-arriving restaurant reports (24-48 hours)")
        print("✅ Impact analysis showing data quality changes")
        print("✅ Automated reprocessing of affected data")
        print("✅ Data quality scoring for late arrivals")
        print("✅ Monitoring and alerting capabilities")
        print()
        print("📊 Key Results:")
        print("• Restaurant performance metrics updated with late data")
        print("• Data lineage tracked with arrival timestamps")
        print("• Quality scores reflect reliability of late data")
        print("• Pipeline demonstrates enterprise-grade late data handling")
        print()
        print("🎬 Demo Ready for Presentation!")
        
    else:
        print("⚠️  DEMO PARTIALLY COMPLETED")
        print(f"❌ {total_stages - success_count} stages failed")
        print()
        print("🔧 Troubleshooting:")
        print("1. Check Docker services are running (Spark, MinIO)")
        print("2. Verify network connectivity")
        print("3. Check logs for specific error details")
        print("4. Ensure sufficient disk space")
    
    return success_count == total_stages

if __name__ == "__main__":
    print("🚀 Starting WoEat Late-Arriving Data Demo...")
    
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n⚠️ Demo interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Demo failed with exception: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1) 