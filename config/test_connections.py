import snowflake.connector
from dotenv import load_dotenv
import os

# Load .env file
load_dotenv('config/.env')

print("Testing Snowflake connection...")

try:
    conn = snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        role=os.getenv('SNOWFLAKE_ROLE')
    )
    
    cursor = conn.cursor()
    cursor.execute("SELECT CURRENT_VERSION(), CURRENT_DATABASE(), CURRENT_WAREHOUSE()")
    row = cursor.fetchone()
    
    print(f"✅ Snowflake connected successfully!")
    print(f"   Version  : {row[0]}")
    print(f"   Database : {row[1]}")
    print(f"   Warehouse: {row[2]}")
    
    # Verify schemas exist
    cursor.execute("SHOW SCHEMAS IN DATABASE DEV_ECOSYSTEM_DB")
    schemas = cursor.fetchall()
    print(f"\n✅ Schemas found:")
    for schema in schemas:
        print(f"   - {schema[1]}")
    
    conn.close()
    print("\n✅ Connection closed cleanly.")

except Exception as e:
    print(f"❌ Connection failed: {e}")