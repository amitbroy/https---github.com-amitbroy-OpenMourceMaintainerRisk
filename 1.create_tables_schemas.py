import os
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

def get_connection():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE")
    )

def create_schemas():
    """Create all schemas"""
    conn = get_connection()
    cursor = conn.cursor()
    
    schemas = ['RAW', 'STAGE', 'LINKMAP' ,'ENRICH', 'CURATE', 'ORCHESTRATION']
    for schema in schemas:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    
    cursor.close()
    conn.close()
    print("Schemas created: RAW, STAGE, LINKMAP, ENRICH, CURATE, ORCHESTRATION")

def create_raw_tables():
    """Create all raw tables"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # git_repositories
    cursor.execute("""
    CREATE OR REPLACE TABLE RAW.SRC_GIT_REPOSITORIES (
        data_source VARCHAR(200),
        id VARCHAR(100),
        name VARCHAR(200),
        full_name VARCHAR(400),
        owner VARCHAR(200),
        language VARCHAR(100),
        stars INTEGER DEFAULT 0,
        forks INTEGER DEFAULT 0,
        html_url VARCHAR(500),
        created_at TIMESTAMP_NTZ,
        updated_at TIMESTAMP_NTZ,
        load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
    """)

    # git_repo_contributors
    cursor.execute("""
    CREATE OR REPLACE TABLE RAW.SRC_GIT_REPO_CONTRIBUTORS (
        data_source VARCHAR(400),
        repo_full_name VARCHAR(400),
        contributor VARCHAR(200),
        total_commits INTEGER DEFAULT 0,
        recent_90_days_commits INTEGER DEFAULT 0,
        load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
    """)
    
    # git_repo_commits
    cursor.execute("""
    CREATE OR REPLACE TABLE RAW.SRC_GIT_REPO_COMMITS (
        data_source VARCHAR(200),
        repo VARCHAR(400),
        commits_30d INTEGER DEFAULT 0,
        commits_90d INTEGER DEFAULT 0,
        commits_180d INTEGER DEFAULT 0,
        last_commit_date TIMESTAMP_NTZ,
        load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
    """)
    
    # git_repo_issues
    cursor.execute("""
    CREATE OR REPLACE TABLE RAW.SRC_GIT_REPO_ISSUES (
        data_source VARCHAR(200),
        repo VARCHAR(400),
        open_issues INTEGER DEFAULT 0,
        closed_issues INTEGER DEFAULT 0,
        issues_last_90d INTEGER DEFAULT 0,
        load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
    """)
    
    # git_repo_releases
    cursor.execute("""
    CREATE OR REPLACE TABLE RAW.SRC_GIT_REPO_RELEASES (
        data_source VARCHAR(200),
        repo VARCHAR(400),
        release_count INTEGER DEFAULT 0,
        last_release_date TIMESTAMP_NTZ,
        days_since_last_release INTEGER DEFAULT 999,
        load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
    """)
    
    cursor.close()
    conn.close()
    print("raw tables created")

def create_stage_tables():
    """Create stage tables"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # git_repositories
    cursor.execute("""
    CREATE OR REPLACE TABLE STAGE.STG_REPOSITORIES (
        data_source VARCHAR(200),
        id VARCHAR(100),
        name VARCHAR(200),
        full_name VARCHAR(400),
        owner VARCHAR(200),
        language VARCHAR(100),
        stars INTEGER DEFAULT 0,
        forks INTEGER DEFAULT 0,
        html_url VARCHAR(500),
        created_at TIMESTAMP_NTZ,
        updated_at TIMESTAMP_NTZ,
        load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        valid_flag BOOLEAN,
        invalid_reason VARCHAR(500)
    )
    """)
    cursor.close()
    conn.close()
    print("Stage table is created")

def create_linkmap_tables():
    """Create linkmap tables"""
    conn = get_connection()
    cursor = conn.cursor()

    # git_repo_contributors
    cursor.execute("""
    CREATE OR REPLACE TABLE LINKMAP.HUB_REPO_CONTRIBUTORS (
        data_source VARCHAR(200),
        repo_full_name VARCHAR(400),
        contributor VARCHAR(200),
        total_commits INTEGER DEFAULT 0,
        recent_90_days_commits INTEGER DEFAULT 0,
        load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
    """)
    
    # git_repo_commits
    cursor.execute("""
    CREATE OR REPLACE TABLE LINKMAP.HUB_REPO_COMMITS (
        data_source VARCHAR(200),
        repo VARCHAR(400),
        commits_30d INTEGER DEFAULT 0,
        commits_90d INTEGER DEFAULT 0,
        commits_180d INTEGER DEFAULT 0,
        last_commit_date TIMESTAMP_NTZ,
        load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
    """)
    
    # git_repo_issues
    cursor.execute("""
    CREATE OR REPLACE TABLE LINKMAP.HUB_REPO_ISSUES (
        data_source VARCHAR(200),
        repo VARCHAR(400),
        open_issues INTEGER DEFAULT 0,
        closed_issues INTEGER DEFAULT 0,
        issues_last_90d INTEGER DEFAULT 0,
        load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
    """)
    
    # git_repo_releases
    cursor.execute("""
    CREATE OR REPLACE TABLE LINKMAP.HUB_REPO_RELEASES (
        data_source VARCHAR(200),
        repo VARCHAR(400),
        release_count INTEGER DEFAULT 0,
        last_release_date TIMESTAMP_NTZ,
        days_since_last_release INTEGER DEFAULT 999,
        load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
    """)
    
    cursor.close()
    conn.close()
    print("linkmap tables created")

def create_enrich_table():
    """Create enrich.repo_entryline table"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
    CREATE OR REPLACE TABLE ENRICH.REPO_ENTRYLINE (
        data_source VARCHAR(200),
        id VARCHAR(100),
        name VARCHAR(200),
        full_name VARCHAR(400),
        owner VARCHAR(200),
        language VARCHAR(100),
        stars INTEGER DEFAULT 0,
        forks INTEGER DEFAULT 0,
        html_url VARCHAR(500),
        created_at TIMESTAMP_NTZ,
        updated_at TIMESTAMP_NTZ,
        commits_30d INTEGER DEFAULT 0,
        commits_90d INTEGER DEFAULT 0,
        commits_180d INTEGER DEFAULT 0,
        last_commit_date TIMESTAMP_NTZ,
        total_contributors INTEGER DEFAULT 0,
        active_contributors_90d INTEGER DEFAULT 0,
        open_issues INTEGER DEFAULT 0,
        closed_issues INTEGER DEFAULT 0,
        issues_last_90d INTEGER DEFAULT 0,
        release_count INTEGER DEFAULT 0,
        last_release_date TIMESTAMP_NTZ,
        days_since_last_release INTEGER DEFAULT 999,
        enriched_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
    """)
    
    cursor.close()
    conn.close()
    print("Enrich table created")

def create_curate_table():
    """Create curate table"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
    CREATE OR REPLACE TABLE CURATE.RISK_ANALYSIS_DATA_PRODUCT (
        data_source VARCHAR(200),
        full_name VARCHAR(400),
        language VARCHAR(100),
        stars INTEGER DEFAULT 0,
        commits_90d INTEGER DEFAULT 0,
        active_contributors_90d INTEGER DEFAULT 0,
        days_since_last_release INTEGER DEFAULT 999,
        open_issues INTEGER DEFAULT 0,
        risk_score FLOAT DEFAULT 0.0,
        risk_category VARCHAR(20),
        last_updated TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
    """)
    
    cursor.close()
    conn.close()
    print("Curate table created")

def main():
    """Main function"""
    create_schemas()
    create_raw_tables()
    create_stage_tables()
    create_linkmap_tables()
    create_enrich_table()
    create_curate_table()

if __name__ == "__main__":
    main()