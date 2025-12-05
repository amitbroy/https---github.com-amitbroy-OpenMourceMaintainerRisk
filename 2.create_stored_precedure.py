
import snowflake.connector
import os
from dotenv import load_dotenv

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

# Define all stored procedures
STORED_PROCEDURES = [
    {
        "name": "STAGE.SP_LOAD_STG_REPOSITORIES",
        "code": """
CREATE OR REPLACE PROCEDURE STAGE.SP_LOAD_STG_REPOSITORIES()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_total_records INTEGER;
    v_valid_records INTEGER;
    v_invalid_records INTEGER;
    v_duplicate_count INTEGER;
    v_final_records INTEGER;
    v_result VARCHAR;
BEGIN
    -- Get total records count
    SELECT COUNT(*) INTO v_total_records 
    FROM RAW.SRC_GIT_REPOSITORIES 
    WHERE DATA_SOURCE = 'git_hub';
    
    -- Count records that fail data quality checks
    SELECT COUNT(*) INTO v_invalid_records 
    FROM RAW.SRC_GIT_REPOSITORIES 
    WHERE DATA_SOURCE = 'git_hub'
      AND (
          ID IS NULL 
          OR TRIM(ID) = ''
          OR NAME IS NULL 
          OR TRIM(NAME) = ''
          OR FULL_NAME IS NULL 
          OR TRIM(FULL_NAME) = ''
          OR STARS < 0
          OR FORKS < 0
          OR (CREATED_AT IS NULL OR TO_VARCHAR(CREATED_AT) = '')
      );
    
    v_valid_records := v_total_records - v_invalid_records;
    
    CREATE OR REPLACE TEMPORARY TABLE TEMP_CLEANED_REPOS AS
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(FULL_NAME) 
            ORDER BY 
                CASE 
                    WHEN UPDATED_AT IS NULL THEN '1970-01-01 00:00:00'::TIMESTAMP_NTZ
                    ELSE UPDATED_AT 
                END DESC
        ) as rn
    FROM (
        SELECT 
            DATA_SOURCE,
            TRIM(ID) AS ID,
            CASE WHEN TRIM(NAME) = '' OR NAME IS NULL THEN 'UNKNOWN' ELSE TRIM(NAME) END AS NAME,
            CASE WHEN TRIM(FULL_NAME) = '' OR FULL_NAME IS NULL THEN 'UNKNOWN/UNKNOWN' ELSE TRIM(FULL_NAME) END AS FULL_NAME,
            CASE WHEN TRIM(OWNER) = '' OR OWNER IS NULL THEN 'UNKNOWN' ELSE TRIM(OWNER) END AS OWNER,
            CASE WHEN TRIM(LANGUAGE) = '' OR LANGUAGE IS NULL THEN 'Unknown' ELSE TRIM(LANGUAGE) END AS LANGUAGE,
            CASE WHEN STARS < 0 THEN 0 ELSE STARS END AS STARS,
            CASE WHEN FORKS < 0 THEN 0 ELSE FORKS END AS FORKS,
            CASE WHEN TRIM(HTML_URL) = '' OR HTML_URL IS NULL THEN 'https://github.com/UNKNOWN' ELSE TRIM(HTML_URL) END AS HTML_URL,
            CASE 
                WHEN CREATED_AT IS NULL OR TO_VARCHAR(CREATED_AT) = '' 
                THEN CURRENT_TIMESTAMP() - INTERVAL '365 DAY'
                ELSE CREATED_AT
            END AS CREATED_AT,
            CASE 
                WHEN UPDATED_AT IS NULL OR TO_VARCHAR(UPDATED_AT) = '' 
                THEN CASE 
                    WHEN CREATED_AT IS NULL OR TO_VARCHAR(CREATED_AT) = '' 
                    THEN CURRENT_TIMESTAMP()
                    ELSE CREATED_AT
                END
                ELSE UPDATED_AT
            END AS UPDATED_AT,
            CASE 
                WHEN ID IS NOT NULL AND TRIM(ID) != '' 
                     AND NAME IS NOT NULL AND TRIM(NAME) != ''
                     AND FULL_NAME IS NOT NULL AND TRIM(FULL_NAME) != ''
                     AND STARS >= 0
                     AND FORKS >= 0
                THEN TRUE
                ELSE FALSE
            END AS VALID_FLAG,
            CASE 
                WHEN ID IS NULL OR TRIM(ID) = '' THEN 'Missing ID'
                WHEN NAME IS NULL OR TRIM(NAME) = '' THEN 'Missing Name'
                WHEN FULL_NAME IS NULL OR TRIM(FULL_NAME) = '' THEN 'Missing Full Name'
                WHEN STARS < 0 THEN 'Negative Stars Count'
                WHEN FORKS < 0 THEN 'Negative Forks Count'
                ELSE 'Valid'
            END AS INVALID_REASON
        FROM RAW.SRC_GIT_REPOSITORIES
        WHERE DATA_SOURCE = 'git_hub'
    );
    
    SELECT COUNT(*) - COUNT(DISTINCT FULL_NAME) INTO v_duplicate_count 
    FROM TEMP_CLEANED_REPOS;
    
    
    INSERT INTO STAGE.STG_REPOSITORIES (
        DATA_SOURCE, ID, NAME, FULL_NAME, OWNER, LANGUAGE, STARS, FORKS, HTML_URL, 
        CREATED_AT, UPDATED_AT, LOAD_TIMESTAMP, VALID_FLAG, INVALID_REASON
    )
    SELECT 
        DATA_SOURCE, ID, NAME, FULL_NAME, OWNER, LANGUAGE, STARS, FORKS, HTML_URL,
        CREATED_AT, UPDATED_AT, CURRENT_TIMESTAMP(), VALID_FLAG, INVALID_REASON
    FROM TEMP_CLEANED_REPOS
    WHERE rn = 1;
    
    v_final_records := SQLROWCOUNT;
    
    v_result := 'SUCCESS: Total=' || v_total_records || 
                ', Valid=' || v_valid_records || 
                ', Invalid=' || v_invalid_records || 
                ', DuplicatesRemoved=' || v_duplicate_count ||
                ', FinalLoaded=' || v_final_records || 
                ' records to STAGE.STG_REPOSITORIES';
    
    DROP TABLE TEMP_CLEANED_REPOS;
    
    RETURN v_result;
EXCEPTION
    WHEN OTHER THEN
        SYSTEM$LOG('error', 'SP_LOAD_STG_REPOSITORIES failed: ' || SQLERRM);
        RAISE;
END;
$$;
"""
    },
    {
        "name": "LINKMAP.SP_LOAD_HUB_REPO_CONTRIBUTORS",
        "code": """
CREATE OR REPLACE PROCEDURE LINKMAP.SP_LOAD_HUB_REPO_CONTRIBUTORS()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    
    INSERT INTO LINKMAP.HUB_REPO_CONTRIBUTORS (
        DATA_SOURCE, REPO_FULL_NAME, CONTRIBUTOR, TOTAL_COMMITS, RECENT_90_DAYS_COMMITS, LOAD_TIMESTAMP
    )
    SELECT 
        DATA_SOURCE,
        CASE WHEN TRIM(REPO_FULL_NAME) = '' OR REPO_FULL_NAME IS NULL THEN 'UNKNOWN/UNKNOWN' ELSE TRIM(REPO_FULL_NAME) END AS REPO_FULL_NAME,
        CASE WHEN TRIM(CONTRIBUTOR) = '' OR CONTRIBUTOR IS NULL THEN 'unknown_contributor' ELSE TRIM(CONTRIBUTOR) END AS CONTRIBUTOR,
        CASE WHEN TOTAL_COMMITS < 0 THEN 0 ELSE TOTAL_COMMITS END AS TOTAL_COMMITS,
        CASE 
            WHEN RECENT_90_DAYS_COMMITS < 0 THEN 0
            WHEN RECENT_90_DAYS_COMMITS > TOTAL_COMMITS THEN TOTAL_COMMITS
            ELSE RECENT_90_DAYS_COMMITS 
        END AS RECENT_90_DAYS_COMMITS,
        CURRENT_TIMESTAMP() AS LOAD_TIMESTAMP
    FROM RAW.SRC_GIT_REPO_CONTRIBUTORS
    WHERE DATA_SOURCE = 'git_hub';
    
    RETURN 'SUCCESS: Loaded ' || SQLROWCOUNT || ' records to LINKMAP.HUB_REPO_CONTRIBUTORS';
END;
$$;
"""
    },
    {
        "name": "LINKMAP.SP_LOAD_HUB_REPO_COMMITS",
        "code": """
CREATE OR REPLACE PROCEDURE LINKMAP.SP_LOAD_HUB_REPO_COMMITS()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    
    INSERT INTO LINKMAP.HUB_REPO_COMMITS (
        DATA_SOURCE, REPO, COMMITS_30D, COMMITS_90D, COMMITS_180D, LAST_COMMIT_DATE, LOAD_TIMESTAMP
    )
    SELECT 
        DATA_SOURCE,
        CASE WHEN TRIM(REPO) = '' OR REPO IS NULL THEN 'UNKNOWN/UNKNOWN' ELSE TRIM(REPO) END AS REPO,
        CASE 
            WHEN COMMITS_30D < 0 THEN 0
            WHEN COMMITS_30D > COMMITS_90D THEN COMMITS_90D
            ELSE COMMITS_30D 
        END AS COMMITS_30D,
        CASE 
            WHEN COMMITS_90D < 0 THEN 0
            WHEN COMMITS_90D > COMMITS_180D THEN COMMITS_180D
            ELSE COMMITS_90D 
        END AS COMMITS_90D,
        CASE WHEN COMMITS_180D < 0 THEN 0 ELSE COMMITS_180D END AS COMMITS_180D,
        CASE 
            WHEN LAST_COMMIT_DATE IS NULL OR TO_VARCHAR(LAST_COMMIT_DATE) = '' 
            THEN CURRENT_TIMESTAMP()
            ELSE LAST_COMMIT_DATE
        END AS LAST_COMMIT_DATE,
        CURRENT_TIMESTAMP() AS LOAD_TIMESTAMP
    FROM RAW.SRC_GIT_REPO_COMMITS
    WHERE DATA_SOURCE = 'git_hub';
    
    RETURN 'SUCCESS: Loaded ' || SQLROWCOUNT || ' records to LINKMAP.HUB_REPO_COMMITS';
END;
$$;
"""
    },
    {
        "name": "LINKMAP.SP_LOAD_HUB_REPO_ISSUES",
        "code": """
CREATE OR REPLACE PROCEDURE LINKMAP.SP_LOAD_HUB_REPO_ISSUES()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    
    INSERT INTO LINKMAP.HUB_REPO_ISSUES (
        DATA_SOURCE, REPO, OPEN_ISSUES, CLOSED_ISSUES, ISSUES_LAST_90D, LOAD_TIMESTAMP
    )
    SELECT 
        DATA_SOURCE,
        CASE WHEN TRIM(REPO) = '' OR REPO IS NULL THEN 'UNKNOWN/UNKNOWN' ELSE TRIM(REPO) END AS REPO,
        CASE WHEN OPEN_ISSUES < 0 THEN 0 ELSE OPEN_ISSUES END AS OPEN_ISSUES,
        CASE WHEN CLOSED_ISSUES < 0 THEN 0 ELSE CLOSED_ISSUES END AS CLOSED_ISSUES,
        CASE 
            WHEN ISSUES_LAST_90D < 0 THEN 0
            WHEN ISSUES_LAST_90D > (OPEN_ISSUES + CLOSED_ISSUES) THEN (OPEN_ISSUES + CLOSED_ISSUES)
            ELSE ISSUES_LAST_90D 
        END AS ISSUES_LAST_90D,
        CURRENT_TIMESTAMP() AS LOAD_TIMESTAMP
    FROM RAW.SRC_GIT_REPO_ISSUES
    WHERE DATA_SOURCE = 'git_hub';
    
    RETURN 'SUCCESS: Loaded ' || SQLROWCOUNT || ' records to LINKMAP.HUB_REPO_ISSUES';
END;
$$;
"""
    },
    {
        "name": "LINKMAP.SP_LOAD_HUB_REPO_RELEASES",
        "code": """
CREATE OR REPLACE PROCEDURE LINKMAP.SP_LOAD_HUB_REPO_RELEASES()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    
    INSERT INTO LINKMAP.HUB_REPO_RELEASES (
        DATA_SOURCE, REPO, RELEASE_COUNT, LAST_RELEASE_DATE, DAYS_SINCE_LAST_RELEASE, LOAD_TIMESTAMP
    )
    SELECT 
        DATA_SOURCE,
        CASE WHEN TRIM(REPO) = '' OR REPO IS NULL THEN 'UNKNOWN/UNKNOWN' ELSE TRIM(REPO) END AS REPO,
        CASE WHEN RELEASE_COUNT < 0 THEN 0 ELSE RELEASE_COUNT END AS RELEASE_COUNT,
        CASE 
            WHEN LAST_RELEASE_DATE IS NULL OR TO_VARCHAR(LAST_RELEASE_DATE) = '' 
            THEN '1970-01-01 00:00:00'::TIMESTAMP_NTZ
            ELSE LAST_RELEASE_DATE
        END AS LAST_RELEASE_DATE,
        CASE WHEN DAYS_SINCE_LAST_RELEASE < 0 THEN 999 ELSE DAYS_SINCE_LAST_RELEASE END AS DAYS_SINCE_LAST_RELEASE,
        CURRENT_TIMESTAMP() AS LOAD_TIMESTAMP
    FROM RAW.SRC_GIT_REPO_RELEASES
    WHERE DATA_SOURCE = 'git_hub';
    
    RETURN 'SUCCESS: Loaded ' || SQLROWCOUNT || ' records to LINKMAP.HUB_REPO_RELEASES';
END;
$$;
"""
    },
    {
        "name": "ENRICH.SP_LOAD_REPO_ENTRYLINE",
        "code": """
CREATE OR REPLACE PROCEDURE ENRICH.SP_LOAD_REPO_ENTRYLINE()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_total_enriched INTEGER;
    v_result VARCHAR;
BEGIN
    
    INSERT INTO ENRICH.REPO_ENTRYLINE (
        DATA_SOURCE, ID, NAME, FULL_NAME, OWNER, LANGUAGE, STARS, FORKS, HTML_URL,
        CREATED_AT, UPDATED_AT, COMMITS_30D, COMMITS_90D, COMMITS_180D, LAST_COMMIT_DATE,
        TOTAL_CONTRIBUTORS, ACTIVE_CONTRIBUTORS_90D, OPEN_ISSUES, CLOSED_ISSUES, ISSUES_LAST_90D,
        RELEASE_COUNT, LAST_RELEASE_DATE, DAYS_SINCE_LAST_RELEASE, ENRICHED_AT
    )
    WITH 
    contributors_agg AS (
        SELECT 
            REPO_FULL_NAME,
            COUNT(DISTINCT CONTRIBUTOR) as total_contributors,
            COUNT(DISTINCT CASE WHEN RECENT_90_DAYS_COMMITS > 0 THEN CONTRIBUTOR END) as active_contributors_90d
        FROM LINKMAP.HUB_REPO_CONTRIBUTORS
        WHERE DATA_SOURCE = 'git_hub'
        GROUP BY REPO_FULL_NAME
    ),
    commits_data AS (
        SELECT REPO, COMMITS_30D, COMMITS_90D, COMMITS_180D, LAST_COMMIT_DATE
        FROM LINKMAP.HUB_REPO_COMMITS WHERE DATA_SOURCE = 'git_hub'
    ),
    issues_data AS (
        SELECT REPO, OPEN_ISSUES, CLOSED_ISSUES, ISSUES_LAST_90D
        FROM LINKMAP.HUB_REPO_ISSUES WHERE DATA_SOURCE = 'git_hub'
    ),
    releases_data AS (
        SELECT REPO, RELEASE_COUNT, LAST_RELEASE_DATE, DAYS_SINCE_LAST_RELEASE
        FROM LINKMAP.HUB_REPO_RELEASES WHERE DATA_SOURCE = 'git_hub'
    )
    SELECT 
        sr.DATA_SOURCE, sr.ID, sr.NAME, sr.FULL_NAME, sr.OWNER, sr.LANGUAGE, sr.STARS, sr.FORKS, sr.HTML_URL,
        sr.CREATED_AT, sr.UPDATED_AT,
        COALESCE(cd.COMMITS_30D, 0) AS COMMITS_30D,
        COALESCE(cd.COMMITS_90D, 0) AS COMMITS_90D,
        COALESCE(cd.COMMITS_180D, 0) AS COMMITS_180D,
        cd.LAST_COMMIT_DATE,
        COALESCE(ca.total_contributors, 0) AS TOTAL_CONTRIBUTORS,
        COALESCE(ca.active_contributors_90d, 0) AS ACTIVE_CONTRIBUTORS_90D,
        COALESCE(id.OPEN_ISSUES, 0) AS OPEN_ISSUES,
        COALESCE(id.CLOSED_ISSUES, 0) AS CLOSED_ISSUES,
        COALESCE(id.ISSUES_LAST_90D, 0) AS ISSUES_LAST_90D,
        COALESCE(rd.RELEASE_COUNT, 0) AS RELEASE_COUNT,
        rd.LAST_RELEASE_DATE,
        COALESCE(rd.DAYS_SINCE_LAST_RELEASE, 999) AS DAYS_SINCE_LAST_RELEASE,
        CURRENT_TIMESTAMP() AS ENRICHED_AT
    FROM STAGE.STG_REPOSITORIES sr
    LEFT JOIN contributors_agg ca ON sr.FULL_NAME = ca.REPO_FULL_NAME
    LEFT JOIN commits_data cd ON sr.FULL_NAME = cd.REPO
    LEFT JOIN issues_data id ON sr.FULL_NAME = id.REPO
    LEFT JOIN releases_data rd ON sr.FULL_NAME = rd.REPO
    WHERE sr.DATA_SOURCE = 'git_hub' AND sr.VALID_FLAG = TRUE;
    
    v_total_enriched := SQLROWCOUNT;
    v_result := 'SUCCESS: Enriched ' || v_total_enriched || ' records into ENRICH.REPO_ENTRYLINE';
    RETURN v_result;
EXCEPTION
    WHEN OTHER THEN
        SYSTEM$LOG('error', 'SP_LOAD_REPO_ENTRYLINE failed: ' || SQLERRM);
        RAISE;
END;
$$;
"""
    },
    {
        "name": "CURATE.SP_LOAD_RISK_ANALYSIS_DATA_PRODUCT",
        "code": """
CREATE OR REPLACE PROCEDURE CURATE.SP_LOAD_RISK_ANALYSIS_DATA_PRODUCT()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_total_records INTEGER;
    v_high_risk_count INTEGER;
    v_medium_risk_count INTEGER;
    v_low_risk_count INTEGER;
    v_result VARCHAR;
BEGIN
    
    INSERT INTO CURATE.RISK_ANALYSIS_DATA_PRODUCT (
        DATA_SOURCE, FULL_NAME, LANGUAGE, STARS, COMMITS_90D, ACTIVE_CONTRIBUTORS_90D,
        DAYS_SINCE_LAST_RELEASE, OPEN_ISSUES, RISK_SCORE, RISK_CATEGORY, LAST_UPDATED
    )
    WITH risk_calc AS (
        SELECT 
            DATA_SOURCE, FULL_NAME, LANGUAGE, STARS, COMMITS_90D, ACTIVE_CONTRIBUTORS_90D,
            DAYS_SINCE_LAST_RELEASE, OPEN_ISSUES,
            CASE 
                WHEN STARS >= 10000 THEN 0.0
                WHEN STARS >= 1000 THEN 0.2
                WHEN STARS >= 100 THEN 0.4
                WHEN STARS >= 10 THEN 0.6
                WHEN STARS >= 1 THEN 0.8
                ELSE 1.0
            END AS stars_risk_factor,
            CASE 
                WHEN COMMITS_90D >= 100 THEN 0.0
                WHEN COMMITS_90D >= 50 THEN 0.2
                WHEN COMMITS_90D >= 20 THEN 0.4
                WHEN COMMITS_90D >= 5 THEN 0.6
                WHEN COMMITS_90D >= 1 THEN 0.8
                ELSE 1.0
            END AS commits_risk_factor,
            CASE 
                WHEN ACTIVE_CONTRIBUTORS_90D >= 10 THEN 0.0
                WHEN ACTIVE_CONTRIBUTORS_90D >= 5 THEN 0.2
                WHEN ACTIVE_CONTRIBUTORS_90D >= 3 THEN 0.4
                WHEN ACTIVE_CONTRIBUTORS_90D >= 1 THEN 0.6
                ELSE 1.0
            END AS contributors_risk_factor,
            CASE 
                WHEN DAYS_SINCE_LAST_RELEASE <= 30 THEN 0.0
                WHEN DAYS_SINCE_LAST_RELEASE <= 90 THEN 0.3
                WHEN DAYS_SINCE_LAST_RELEASE <= 180 THEN 0.6
                WHEN DAYS_SINCE_LAST_RELEASE <= 365 THEN 0.8
                ELSE 1.0
            END AS release_risk_factor,
            CASE 
                WHEN OPEN_ISSUES = 0 THEN 0.0
                WHEN OPEN_ISSUES <= 5 THEN 0.2
                WHEN OPEN_ISSUES <= 10 THEN 0.4
                WHEN OPEN_ISSUES <= 20 THEN 0.6
                WHEN OPEN_ISSUES <= 50 THEN 0.8
                ELSE 1.0
            END AS issues_risk_factor
        FROM ENRICH.REPO_ENTRYLINE
        WHERE DATA_SOURCE = 'git_hub'
    ),
    weighted_risk AS (
        SELECT 
            DATA_SOURCE, FULL_NAME, LANGUAGE, STARS, COMMITS_90D, ACTIVE_CONTRIBUTORS_90D,
            DAYS_SINCE_LAST_RELEASE, OPEN_ISSUES,
            ROUND(
                (stars_risk_factor * 0.15) + (commits_risk_factor * 0.25) + 
                (contributors_risk_factor * 0.20) + (release_risk_factor * 0.25) + 
                (issues_risk_factor * 0.15), 2
            ) * 100 as risk_score_raw
        FROM risk_calc
    )
    SELECT 
        DATA_SOURCE, FULL_NAME, LANGUAGE, STARS, COMMITS_90D, ACTIVE_CONTRIBUTORS_90D,
        DAYS_SINCE_LAST_RELEASE, OPEN_ISSUES,
        risk_score_raw as RISK_SCORE,
        CASE 
            WHEN risk_score_raw >= 70 THEN 'HIGH'
            WHEN risk_score_raw >= 40 THEN 'MEDIUM'
            ELSE 'LOW'
        END as RISK_CATEGORY,
        CURRENT_TIMESTAMP() as LAST_UPDATED
    FROM weighted_risk;
    
    v_total_records := SQLROWCOUNT;
    
    SELECT 
        COUNT(CASE WHEN RISK_CATEGORY = 'HIGH' THEN 1 END),
        COUNT(CASE WHEN RISK_CATEGORY = 'MEDIUM' THEN 1 END),
        COUNT(CASE WHEN RISK_CATEGORY = 'LOW' THEN 1 END)
    INTO v_high_risk_count, v_medium_risk_count, v_low_risk_count
    FROM CURATE.RISK_ANALYSIS_DATA_PRODUCT
    WHERE DATA_SOURCE = 'git_hub';
    
    v_result := 'SUCCESS: Created ' || v_total_records || ' risk analysis records. ' ||
                'High Risk: ' || v_high_risk_count || ' | ' ||
                'Medium Risk: ' || v_medium_risk_count || ' | ' ||
                'Low Risk: ' || v_low_risk_count;
    
    RETURN v_result;
EXCEPTION
    WHEN OTHER THEN
        SYSTEM$LOG('error', 'SP_LOAD_RISK_ANALYSIS_DATA_PRODUCT failed: ' || SQLERRM);
        RAISE;
END;
$$;
"""
    }
]

def create_stored_procedures():
    conn = None
    cursor = None
    
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        for sp in STORED_PROCEDURES:
            try:
                cursor.execute(sp['code'])
                print(f"store procedure created : {sp['name']}")
            except Exception as e:
                print(f"Error creating {sp['name']}: {e}")
                
    except Exception as e:
        print(f"Connection error: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    create_stored_procedures()