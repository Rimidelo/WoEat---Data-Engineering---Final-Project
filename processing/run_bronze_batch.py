from bronze_ingestion import BronzeIngestion

if __name__ == "__main__":
    bronze_ingestion = BronzeIngestion()
    
    try:
        print("🔄 Running Bronze batch ingestion only...")
        
        # Ingest batch data only (no streaming)
        bronze_ingestion.ingest_batch_data()
        
        print("✅ Bronze batch ingestion completed successfully!")
        
    except Exception as e:
        print(f"❌ Error in Bronze batch ingestion: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        bronze_ingestion.stop() 