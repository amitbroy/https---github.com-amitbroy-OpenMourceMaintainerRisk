# setup_stream_pipeline.py
import snowflake.connector
from dotenv import load_dotenv
import os

load_dotenv()

def setup_stream_pipeline():
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse='COMPUTE_WH',
        database=os.getenv("SNOWFLAKE_DATABASE")
    )
    
    cursor = conn.cursor()
    
    try:
        print("1. Creating ORCHESTRATION schema if not exists...")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS ORCHESTRATION;")
        
        print("2. Creating stream on raw table...")
        cursor.execute("""
            CREATE OR REPLACE STREAM RAW.STREAM_SRC_GIT_REPOSITORIES 
            ON TABLE RAW.SRC_GIT_REPOSITORIES;
        """)
        
        print("3. Creating all tasks in ORCHESTRATION schema...")
        
        # Create all tasks in ORCHESTRATION schema
        tasks = [
            # Task 1: Triggered by stream
            """CREATE OR REPLACE TASK ORCHESTRATION.TASK_TRIGGER_PIPELINE
                WAREHOUSE = 'COMPUTE_WH'
                WHEN SYSTEM$STREAM_HAS_DATA('RAW.STREAM_SRC_GIT_REPOSITORIES')
            AS 
                CALL STAGE.SP_LOAD_STG_REPOSITORIES();""",
            
            # Task 2: Load all linkmap tables
            """CREATE OR REPLACE TASK ORCHESTRATION.TASK_LOAD_LINKMAP
                WAREHOUSE = 'COMPUTE_WH'
                AFTER ORCHESTRATION.TASK_TRIGGER_PIPELINE
            AS 
            BEGIN
                CALL LINKMAP.SP_LOAD_HUB_REPO_CONTRIBUTORS();
                CALL LINKMAP.SP_LOAD_HUB_REPO_COMMITS();
                CALL LINKMAP.SP_LOAD_HUB_REPO_ISSUES();
                CALL LINKMAP.SP_LOAD_HUB_REPO_RELEASES();
            END;""",
            
            # Task 3: Load enrich
            """CREATE OR REPLACE TASK ORCHESTRATION.TASK_LOAD_ENRICH
                WAREHOUSE = 'COMPUTE_WH'
                AFTER ORCHESTRATION.TASK_LOAD_LINKMAP
            AS 
                CALL ENRICH.SP_LOAD_REPO_ENTRYLINE();""",
            
            # Task 4: Load curate (final)
            """CREATE OR REPLACE TASK ORCHESTRATION.TASK_LOAD_CURATE
                WAREHOUSE = 'COMPUTE_WH'
                AFTER ORCHESTRATION.TASK_LOAD_ENRICH
            AS 
                CALL CURATE.SP_LOAD_RISK_ANALYSIS_DATA_PRODUCT();"""
        ]
        
        for task_sql in tasks:
            cursor.execute(task_sql)
        
        print("4. Resuming tasks in reverse order...")
        # Resume in reverse order (bottom-up)
        cursor.execute("ALTER TASK ORCHESTRATION.TASK_LOAD_CURATE RESUME;")
        cursor.execute("ALTER TASK ORCHESTRATION.TASK_LOAD_ENRICH RESUME;")
        cursor.execute("ALTER TASK ORCHESTRATION.TASK_LOAD_LINKMAP RESUME;")
        cursor.execute("ALTER TASK ORCHESTRATION.TASK_TRIGGER_PIPELINE RESUME;")
        
        print("✅ Stream pipeline setup complete!")
        print("\nPipeline Flow:")
        print("1. Data inserted into RAW.SRC_GIT_REPOSITORIES")
        print("2. Stream detects changes")
        print("3. TASK_TRIGGER_PIPELINE → TASK_LOAD_LINKMAP → TASK_LOAD_ENRICH → TASK_LOAD_CURATE")
        print("4. Data available in CURATE.RISK_ANALYSIS_DATA_PRODUCT")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    setup_stream_pipeline()