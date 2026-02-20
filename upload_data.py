import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# 1. Load the DB_URL from your .env file
load_dotenv()
db_url = os.getenv("DB_URL")

if not db_url:
    print("❌ Error: DB_URL not found in .env. Did you save the file?")
else:
    # 2. Create the connection engine
    engine = create_engine(db_url)

    # 3. Map your table names to the files inside your 'data' folder
    data_map = {
        'dim_products': 'data/dim_products.csv',
        'marketing_spends_daily': 'data/marketing_spends_daily.csv',
        'fact_transactions': 'data/fact_transactions.csv',
        'customer_interactions': 'data/customer_interactions.csv',
        'external_signals': 'data/external_signals.csv'
    }

    # 4. Loop and Upload
    for table, file_path in data_map.items():
        if os.path.exists(file_path):
            try:
                print(f"⏳ Reading {file_path}...")
                df = pd.read_csv(file_path)
                
                print(f"🚀 Pushing to Supabase table: {table}...")
                # index=False ensures we don't add an extra 'row number' column
                df.to_sql(table, engine, if_exists='replace', index=False)
                print(f"✅ {table} uploaded successfully!")
            except Exception as e:
                print(f"❌ Error uploading {table}: {e}")
        else:
            print(f"⚠️ Warning: File {file_path} not found.")

    print("\n🏁 All tasks complete. Check your Supabase Dashboard!")