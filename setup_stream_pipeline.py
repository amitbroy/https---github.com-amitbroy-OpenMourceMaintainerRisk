# setup_pipeline_with_stream.py
import snowflake.connector
from dotenv import load_dotenv
import os

load_dotenv()

def setup_pipeline_with_stream():
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
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ORCHESTRATION.PIPELINE_LOG (
                log_id NUMBER AUTOINCREMENT,
                pipeline_name VARCHAR(50),
                stage VARCHAR(50),
                status VARCHAR(20),
                message VARCHAR(500),
                start_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                end_time TIMESTAMP_NTZ,
                created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            );
        """)
        
        print("3. Creating stream on raw table...")
        cursor.execute("""
            CREATE OR REPLACE STREAM RAW.STREAM_SRC_GIT_REPOSITORIES 
            ON TABLE RAW.SRC_GIT_REPOSITORIES;
        """)
        
        print("4. Creating wrapper procedure with stream check...")
        cursor.execute("""
            CREATE OR REPLACE PROCEDURE ORCHESTRATION.SP_RUN_GIT_PIPELINE()
            RETURNS VARCHAR
            LANGUAGE SQL
            AS
            $$
            DECLARE
                v_stream_has_data BOOLEAN;
                v_result VARCHAR;
            BEGIN
                -- Check if stream has data
                SELECT SYSTEM$STREAM_HAS_DATA('RAW.STREAM_SRC_GIT_REPOSITORIES') INTO v_stream_has_data;
                
                IF (v_stream_has_data = FALSE) THEN
                    -- No new data, skip execution
                    INSERT INTO ORCHESTRATION.PIPELINE_LOG 
                        (pipeline_name, stage, status, message)
                    VALUES ('GITHUB_RISK', 'CHECK_STREAM', 'SKIPPED', 
                            'No new data in stream, skipping pipeline execution');
                    RETURN 'No new data, pipeline skipped';
                END IF;
                
                -- Log start
                INSERT INTO ORCHESTRATION.PIPELINE_LOG 
                    (pipeline_name, stage, status, message)
                VALUES ('GITHUB_RISK', 'FULL_PIPELINE', 'STARTED', 
                        'New data detected, starting pipeline execution');
                
                -- Execute all pipeline steps
                CALL STAGE.SP_LOAD_STG_REPOSITORIES();
                CALL LINKMAP.SP_LOAD_HUB_REPO_CONTRIBUTORS();
                CALL LINKMAP.SP_LOAD_HUB_REPO_COMMITS();
                CALL LINKMAP.SP_LOAD_HUB_REPO_ISSUES();
                CALL LINKMAP.SP_LOAD_HUB_REPO_RELEASES();
                CALL ENRICH.SP_LOAD_REPO_ENTRYLINE();
                CALL CURATE.SP_LOAD_RISK_ANALYSIS_DATA_PRODUCT();
                
                -- Log success
                UPDATE ORCHESTRATION.PIPELINE_LOG 
                SET status = 'COMPLETED', 
                    message = 'Pipeline completed successfully for new data',
                    end_time = CURRENT_TIMESTAMP()
                WHERE pipeline_name = 'GITHUB_RISK' 
                  AND stage = 'FULL_PIPELINE' 
                  AND status = 'STARTED'
                  AND end_time IS NULL;
                
                RETURN 'Pipeline executed successfully for new data';
            EXCEPTION
                WHEN OTHER THEN
                    -- Log error
                    INSERT INTO ORCHESTRATION.PIPELINE_LOG 
                        (pipeline_name, stage, status, message)
                    VALUES ('GITHUB_RISK', 'FULL_PIPELINE', 'ERROR', 
                            'Pipeline failed: ' || SQLERRM);
                    RAISE;
            END;
            $$;
        """)
        
        print("5. Creating scheduled task...")
        cursor.execute("""
            CREATE OR REPLACE TASK ORCHESTRATION.TASK_RUN_GIT_PIPELINE
                WAREHOUSE = 'COMPUTE_WH'
                SCHEDULE = '2 MINUTE'
            AS
                CALL ORCHESTRATION.SP_RUN_GIT_PIPELINE();
        """)
        
        print("6. Resuming task...")
        cursor.execute("ALTER TASK ORCHESTRATION.TASK_RUN_GIT_PIPELINE RESUME;")

        print("Stream-based pipeline setup completed successfully!")
        

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    setup_pipeline_with_stream()