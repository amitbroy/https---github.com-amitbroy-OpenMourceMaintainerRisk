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

def create_monitoring_view():
    """Create the monitoring view in Snowflake"""
    monitoring_view_sql = """
CREATE OR REPLACE VIEW CURATE.VW_RISK_ANALYSIS_PIPELINE_STATUS
AS
WITH 
-- Raw tables monitoring
raw_counts AS (
    SELECT 
        'RAW' as layer,
        'SRC_GIT_REPOSITORIES' as table_name,
        DATA_SOURCE,
        COUNT(*) as record_count,
        MIN(CREATED_AT) as oldest_record,
        MAX(CREATED_AT) as newest_record,
        SUM(CASE WHEN ID IS NULL OR TRIM(ID) = '' THEN 1 ELSE 0 END) as metric_1,
        SUM(CASE WHEN STARS < 0 THEN 1 ELSE 0 END) as metric_2,
        SUM(CASE WHEN FORKS < 0 THEN 1 ELSE 0 END) as metric_3
    FROM RAW.SRC_GIT_REPOSITORIES
    GROUP BY DATA_SOURCE
    
    UNION ALL
    
    SELECT 
        'RAW' as layer,
        'SRC_GIT_REPO_CONTRIBUTORS' as table_name,
        DATA_SOURCE,
        COUNT(*) as record_count,
        NULL as oldest_record,
        NULL as newest_record,
        SUM(CASE WHEN CONTRIBUTOR IS NULL OR TRIM(CONTRIBUTOR) = '' THEN 1 ELSE 0 END) as metric_1,
        SUM(CASE WHEN TOTAL_COMMITS < 0 THEN 1 ELSE 0 END) as metric_2,
        SUM(CASE WHEN RECENT_90_DAYS_COMMITS < 0 THEN 1 ELSE 0 END) as metric_3
    FROM RAW.SRC_GIT_REPO_CONTRIBUTORS
    GROUP BY DATA_SOURCE
    
    UNION ALL
    
    SELECT 
        'RAW' as layer,
        'SRC_GIT_REPO_COMMITS' as table_name,
        DATA_SOURCE,
        COUNT(*) as record_count,
        NULL as oldest_record,
        NULL as newest_record,
        0 as metric_1,
        SUM(CASE WHEN COMMITS_30D < 0 THEN 1 ELSE 0 END) as metric_2,
        SUM(CASE WHEN COMMITS_90D < 0 THEN 1 ELSE 0 END) as metric_3
    FROM RAW.SRC_GIT_REPO_COMMITS
    GROUP BY DATA_SOURCE
    
    UNION ALL
    
    SELECT 
        'RAW' as layer,
        'SRC_GIT_REPO_ISSUES' as table_name,
        DATA_SOURCE,
        COUNT(*) as record_count,
        NULL as oldest_record,
        NULL as newest_record,
        0 as metric_1,
        SUM(CASE WHEN OPEN_ISSUES < 0 THEN 1 ELSE 0 END) as metric_2,
        SUM(CASE WHEN CLOSED_ISSUES < 0 THEN 1 ELSE 0 END) as metric_3
    FROM RAW.SRC_GIT_REPO_ISSUES
    GROUP BY DATA_SOURCE
    
    UNION ALL
    
    SELECT 
        'RAW' as layer,
        'SRC_GIT_REPO_RELEASES' as table_name,
        DATA_SOURCE,
        COUNT(*) as record_count,
        NULL as oldest_record,
        NULL as newest_record,
        0 as metric_1,
        SUM(CASE WHEN RELEASE_COUNT < 0 THEN 1 ELSE 0 END) as metric_2,
        SUM(CASE WHEN DAYS_SINCE_LAST_RELEASE < 0 THEN 1 ELSE 0 END) as metric_3
    FROM RAW.SRC_GIT_REPO_RELEASES
    GROUP BY DATA_SOURCE
),

-- Stage table monitoring
stage_counts AS (
    SELECT 
        'STAGE' as layer,
        'STG_REPOSITORIES' as table_name,
        DATA_SOURCE,
        COUNT(*) as record_count,
        MIN(CREATED_AT) as oldest_record,
        MAX(CREATED_AT) as newest_record,
        SUM(CASE WHEN VALID_FLAG = FALSE THEN 1 ELSE 0 END) as metric_1,
        SUM(CASE WHEN STARS = 0 THEN 1 ELSE 0 END) as metric_2,
        SUM(CASE WHEN FORKS = 0 THEN 1 ELSE 0 END) as metric_3
    FROM STAGE.STG_REPOSITORIES
    GROUP BY DATA_SOURCE
),

-- Linkmap tables monitoring (simplified to match column count)
linkmap_counts AS (
    SELECT 
        'LINKMAP' as layer,
        'HUB_REPO_CONTRIBUTORS' as table_name,
        DATA_SOURCE,
        COUNT(*) as record_count,
        NULL as oldest_record,
        NULL as newest_record,
        COUNT(DISTINCT REPO_FULL_NAME) as metric_1,
        COUNT(DISTINCT CONTRIBUTOR) as metric_2,
        ROUND(AVG(TOTAL_COMMITS), 2) as metric_3
    FROM LINKMAP.HUB_REPO_CONTRIBUTORS
    GROUP BY DATA_SOURCE
    
    UNION ALL
    
    SELECT 
        'LINKMAP' as layer,
        'HUB_REPO_COMMITS' as table_name,
        DATA_SOURCE,
        COUNT(*) as record_count,
        MIN(LAST_COMMIT_DATE) as oldest_record,
        MAX(LAST_COMMIT_DATE) as newest_record,
        COUNT(DISTINCT REPO) as metric_1,
        NULL as metric_2,
        ROUND(AVG(COMMITS_90D), 2) as metric_3
    FROM LINKMAP.HUB_REPO_COMMITS
    GROUP BY DATA_SOURCE
    
    UNION ALL
    
    SELECT 
        'LINKMAP' as layer,
        'HUB_REPO_ISSUES' as table_name,
        DATA_SOURCE,
        COUNT(*) as record_count,
        NULL as oldest_record,
        NULL as newest_record,
        COUNT(DISTINCT REPO) as metric_1,
        NULL as metric_2,
        ROUND(AVG(OPEN_ISSUES + CLOSED_ISSUES), 2) as metric_3
    FROM LINKMAP.HUB_REPO_ISSUES
    GROUP BY DATA_SOURCE
    
    UNION ALL
    
    SELECT 
        'LINKMAP' as layer,
        'HUB_REPO_RELEASES' as table_name,
        DATA_SOURCE,
        COUNT(*) as record_count,
        MIN(LAST_RELEASE_DATE) as oldest_record,
        MAX(LAST_RELEASE_DATE) as newest_record,
        COUNT(DISTINCT REPO) as metric_1,
        NULL as metric_2,
        ROUND(AVG(RELEASE_COUNT), 2) as metric_3
    FROM LINKMAP.HUB_REPO_RELEASES
    GROUP BY DATA_SOURCE
),

-- Enrich table monitoring
enrich_counts AS (
    SELECT 
        'ENRICH' as layer,
        'REPO_ENTRYLINE' as table_name,
        DATA_SOURCE,
        COUNT(*) as record_count,
        MIN(CREATED_AT) as oldest_record,
        MAX(CREATED_AT) as newest_record,
        COUNT(DISTINCT LANGUAGE) as metric_1,
        ROUND(AVG(STARS), 2) as metric_2,
        ROUND(AVG(COMMITS_90D), 2) as metric_3
    FROM ENRICH.REPO_ENTRYLINE
    GROUP BY DATA_SOURCE
),

-- Curate table monitoring
curate_counts AS (
    SELECT 
        'CURATE' as layer,
        'RISK_ANALYSIS_DATA_PRODUCT' as table_name,
        DATA_SOURCE,
        COUNT(*) as record_count,
        MIN(LAST_UPDATED) as oldest_record,
        MAX(LAST_UPDATED) as newest_record,
        COUNT(DISTINCT RISK_CATEGORY) as metric_1,
        ROUND(AVG(RISK_SCORE), 2) as metric_2,
        SUM(CASE WHEN RISK_CATEGORY = 'HIGH' THEN 1 ELSE 0 END) as metric_3
    FROM CURATE.RISK_ANALYSIS_DATA_PRODUCT
    GROUP BY DATA_SOURCE
),

-- Combined table statistics
combined_counts AS (
    SELECT * FROM raw_counts
    UNION ALL SELECT * FROM stage_counts
    UNION ALL SELECT * FROM linkmap_counts
    UNION ALL SELECT * FROM enrich_counts
    UNION ALL SELECT * FROM curate_counts
),

-- Data flow statistics per data source
data_flow AS (
    SELECT 
        1 as display_order,
        DATA_SOURCE,
        'Data Pipeline Flow' as metric_name,
        'RAW to STAGE' as description,
        (SELECT COUNT(*) FROM RAW.SRC_GIT_REPOSITORIES r WHERE r.DATA_SOURCE = rc.DATA_SOURCE) as source_value,
        (SELECT COUNT(*) FROM STAGE.STG_REPOSITORIES s WHERE s.DATA_SOURCE = rc.DATA_SOURCE) as target_value,
        ROUND((SELECT COUNT(*) FROM STAGE.STG_REPOSITORIES s WHERE s.DATA_SOURCE = rc.DATA_SOURCE) * 100.0 / 
              NULLIF((SELECT COUNT(*) FROM RAW.SRC_GIT_REPOSITORIES r WHERE r.DATA_SOURCE = rc.DATA_SOURCE), 0), 2) as percent_complete
    FROM (SELECT DISTINCT DATA_SOURCE FROM RAW.SRC_GIT_REPOSITORIES) rc
    
    UNION ALL
    
    SELECT 
        2,
        DATA_SOURCE,
        'Data Pipeline Flow',
        'STAGE to ENRICH',
        (SELECT COUNT(*) FROM STAGE.STG_REPOSITORIES s WHERE s.DATA_SOURCE = rc.DATA_SOURCE AND s.VALID_FLAG = TRUE),
        (SELECT COUNT(*) FROM ENRICH.REPO_ENTRYLINE e WHERE e.DATA_SOURCE = rc.DATA_SOURCE),
        ROUND((SELECT COUNT(*) FROM ENRICH.REPO_ENTRYLINE e WHERE e.DATA_SOURCE = rc.DATA_SOURCE) * 100.0 / 
              NULLIF((SELECT COUNT(*) FROM STAGE.STG_REPOSITORIES s WHERE s.DATA_SOURCE = rc.DATA_SOURCE AND s.VALID_FLAG = TRUE), 0), 2)
    FROM (SELECT DISTINCT DATA_SOURCE FROM STAGE.STG_REPOSITORIES) rc
    
    UNION ALL
    
    SELECT 
        3,
        DATA_SOURCE,
        'Data Pipeline Flow',
        'ENRICH to CURATE',
        (SELECT COUNT(*) FROM ENRICH.REPO_ENTRYLINE e WHERE e.DATA_SOURCE = rc.DATA_SOURCE),
        (SELECT COUNT(*) FROM CURATE.RISK_ANALYSIS_DATA_PRODUCT c WHERE c.DATA_SOURCE = rc.DATA_SOURCE),
        ROUND((SELECT COUNT(*) FROM CURATE.RISK_ANALYSIS_DATA_PRODUCT c WHERE c.DATA_SOURCE = rc.DATA_SOURCE) * 100.0 / 
              NULLIF((SELECT COUNT(*) FROM ENRICH.REPO_ENTRYLINE e WHERE e.DATA_SOURCE = rc.DATA_SOURCE), 0), 2)
    FROM (SELECT DISTINCT DATA_SOURCE FROM ENRICH.REPO_ENTRYLINE) rc
),

-- Latest load timestamps per data source
latest_loads AS (
    SELECT 
        'STAGE' as layer,
        'Last STG_REPOSITORIES Load' as metric_name,
        DATA_SOURCE,
        MAX(LOAD_TIMESTAMP) as metric_value,
        COUNT(*) as record_count
    FROM STAGE.STG_REPOSITORIES
    GROUP BY DATA_SOURCE
    
    UNION ALL
    
    SELECT 
        'LINKMAP' as layer,
        'Last HUB_REPO_CONTRIBUTORS Load' as metric_name,
        DATA_SOURCE,
        MAX(LOAD_TIMESTAMP) as metric_value,
        COUNT(*) as record_count
    FROM LINKMAP.HUB_REPO_CONTRIBUTORS
    GROUP BY DATA_SOURCE
    
    UNION ALL
    
    SELECT 
        'LINKMAP' as layer,
        'Last HUB_REPO_COMMITS Load' as metric_name,
        DATA_SOURCE,
        MAX(LOAD_TIMESTAMP) as metric_value,
        COUNT(*) as record_count
    FROM LINKMAP.HUB_REPO_COMMITS
    GROUP BY DATA_SOURCE
    
    UNION ALL
    
    SELECT 
        'LINKMAP' as layer,
        'Last HUB_REPO_ISSUES Load' as metric_name,
        DATA_SOURCE,
        MAX(LOAD_TIMESTAMP) as metric_value,
        COUNT(*) as record_count
    FROM LINKMAP.HUB_REPO_ISSUES
    GROUP BY DATA_SOURCE
    
    UNION ALL
    
    SELECT 
        'LINKMAP' as layer,
        'Last HUB_REPO_RELEASES Load' as metric_name,
        DATA_SOURCE,
        MAX(LOAD_TIMESTAMP) as metric_value,
        COUNT(*) as record_count
    FROM LINKMAP.HUB_REPO_RELEASES
    GROUP BY DATA_SOURCE
    
    UNION ALL
    
    SELECT 
        'ENRICH' as layer,
        'Last ENRICH Load' as metric_name,
        DATA_SOURCE,
        MAX(ENRICHED_AT) as metric_value,
        COUNT(*) as record_count
    FROM ENRICH.REPO_ENTRYLINE
    GROUP BY DATA_SOURCE
    
    UNION ALL
    
    SELECT 
        'CURATE' as layer,
        'Last CURATE Load' as metric_name,
        DATA_SOURCE,
        MAX(LAST_UPDATED) as metric_value,
        COUNT(*) as record_count
    FROM CURATE.RISK_ANALYSIS_DATA_PRODUCT
    GROUP BY DATA_SOURCE
),

-- Data quality metrics per data source
quality_metrics AS (
    SELECT 
        'Data Quality' as category,
        'Invalid Records in Stage' as metric_name,
        DATA_SOURCE,
        (SELECT COUNT(*) FROM STAGE.STG_REPOSITORIES s WHERE s.DATA_SOURCE = q.DATA_SOURCE AND s.VALID_FLAG = FALSE) as metric_value,
        ROUND((SELECT COUNT(*) FROM STAGE.STG_REPOSITORIES s WHERE s.DATA_SOURCE = q.DATA_SOURCE AND s.VALID_FLAG = FALSE) * 100.0 / 
              NULLIF((SELECT COUNT(*) FROM STAGE.STG_REPOSITORIES s WHERE s.DATA_SOURCE = q.DATA_SOURCE), 0), 2) as metric_percent
    FROM (SELECT DISTINCT DATA_SOURCE FROM STAGE.STG_REPOSITORIES) q
    
    UNION ALL
    
    SELECT 
        'Data Quality',
        'Repos with No Recent Commits',
        DATA_SOURCE,
        (SELECT COUNT(*) FROM ENRICH.REPO_ENTRYLINE e WHERE e.DATA_SOURCE = q.DATA_SOURCE AND e.COMMITS_90D = 0),
        ROUND((SELECT COUNT(*) FROM ENRICH.REPO_ENTRYLINE e WHERE e.DATA_SOURCE = q.DATA_SOURCE AND e.COMMITS_90D = 0) * 100.0 / 
              NULLIF((SELECT COUNT(*) FROM ENRICH.REPO_ENTRYLINE e WHERE e.DATA_SOURCE = q.DATA_SOURCE), 0), 2)
    FROM (SELECT DISTINCT DATA_SOURCE FROM ENRICH.REPO_ENTRYLINE) q
    
    UNION ALL
    
    SELECT 
        'Data Quality',
        'Repos with No Active Contributors',
        DATA_SOURCE,
        (SELECT COUNT(*) FROM ENRICH.REPO_ENTRYLINE e WHERE e.DATA_SOURCE = q.DATA_SOURCE AND e.ACTIVE_CONTRIBUTORS_90D = 0),
        ROUND((SELECT COUNT(*) FROM ENRICH.REPO_ENTRYLINE e WHERE e.DATA_SOURCE = q.DATA_SOURCE AND e.ACTIVE_CONTRIBUTORS_90D = 0) * 100.0 / 
              NULLIF((SELECT COUNT(*) FROM ENRICH.REPO_ENTRYLINE e WHERE e.DATA_SOURCE = q.DATA_SOURCE), 0), 2)
    FROM (SELECT DISTINCT DATA_SOURCE FROM ENRICH.REPO_ENTRYLINE) q
    
    UNION ALL
    
    SELECT 
        'Data Quality',
        'Repos with No Releases',
        DATA_SOURCE,
        (SELECT COUNT(*) FROM ENRICH.REPO_ENTRYLINE e WHERE e.DATA_SOURCE = q.DATA_SOURCE AND e.RELEASE_COUNT = 0),
        ROUND((SELECT COUNT(*) FROM ENRICH.REPO_ENTRYLINE e WHERE e.DATA_SOURCE = q.DATA_SOURCE AND e.RELEASE_COUNT = 0) * 100.0 / 
              NULLIF((SELECT COUNT(*) FROM ENRICH.REPO_ENTRYLINE e WHERE e.DATA_SOURCE = q.DATA_SOURCE), 0), 2)
    FROM (SELECT DISTINCT DATA_SOURCE FROM ENRICH.REPO_ENTRYLINE) q
),

-- Risk distribution summary per data source
risk_summary AS (
    SELECT 
        'Risk Analysis' as category,
        'High Risk Repositories' as metric_name,
        DATA_SOURCE,
        SUM(CASE WHEN RISK_CATEGORY = 'HIGH' THEN 1 ELSE 0 END) as metric_value,
        ROUND(SUM(CASE WHEN RISK_CATEGORY = 'HIGH' THEN 1 ELSE 0 END) * 100.0 / 
              NULLIF(COUNT(*), 0), 2) as metric_percent
    FROM CURATE.RISK_ANALYSIS_DATA_PRODUCT
    GROUP BY DATA_SOURCE
    
    UNION ALL
    
    SELECT 
        'Risk Analysis',
        'Medium Risk Repositories',
        DATA_SOURCE,
        SUM(CASE WHEN RISK_CATEGORY = 'MEDIUM' THEN 1 ELSE 0 END) as metric_value,
        ROUND(SUM(CASE WHEN RISK_CATEGORY = 'MEDIUM' THEN 1 ELSE 0 END) * 100.0 / 
              NULLIF(COUNT(*), 0), 2) as metric_percent
    FROM CURATE.RISK_ANALYSIS_DATA_PRODUCT
    GROUP BY DATA_SOURCE
    
    UNION ALL
    
    SELECT 
        'Risk Analysis',
        'Low Risk Repositories',
        DATA_SOURCE,
        SUM(CASE WHEN RISK_CATEGORY = 'LOW' THEN 1 ELSE 0 END) as metric_value,
        ROUND(SUM(CASE WHEN RISK_CATEGORY = 'LOW' THEN 1 ELSE 0 END) * 100.0 / 
              NULLIF(COUNT(*), 0), 2) as metric_percent
    FROM CURATE.RISK_ANALYSIS_DATA_PRODUCT
    GROUP BY DATA_SOURCE
    
    UNION ALL
    
    SELECT 
        'Risk Analysis',
        'Average Risk Score',
        DATA_SOURCE,
        ROUND(AVG(RISK_SCORE), 2) as metric_value,
        NULL as metric_percent
    FROM CURATE.RISK_ANALYSIS_DATA_PRODUCT
    GROUP BY DATA_SOURCE
)

-- Main query combining all monitoring data
SELECT * FROM (
    -- Table level statistics
    SELECT 
        1 as section_order,
        'Table Statistics' as section_name,
        rc.layer,
        rc.table_name,
        rc.DATA_SOURCE,
        'Record Count' as metric_name,
        TO_VARCHAR(rc.record_count) as metric_value,
        NULL as metric_percent,
        rc.oldest_record,
        rc.newest_record
    FROM combined_counts rc
    
    UNION ALL
    
    -- Data flow metrics
    SELECT 
        2 as section_order,
        df.metric_name as section_name,
        NULL as layer,
        NULL as table_name,
        df.DATA_SOURCE,
        df.description as metric_name,
        CONCAT(TO_VARCHAR(df.source_value), ' → ', TO_VARCHAR(df.target_value)) as metric_value,
        CONCAT(TO_VARCHAR(df.percent_complete), '%') as metric_percent,
        NULL as oldest_record,
        NULL as newest_record
    FROM data_flow df
    
    UNION ALL
    
    -- Latest load timestamps
    SELECT 
        3 as section_order,
        'Last Load Timestamps' as section_name,
        ll.layer,
        NULL as table_name,
        ll.DATA_SOURCE,
        ll.metric_name,
        TO_VARCHAR(ll.metric_value) as metric_value,
        CONCAT(TO_VARCHAR(ll.record_count), ' records') as metric_percent,
        NULL as oldest_record,
        NULL as newest_record
    FROM latest_loads ll
    
    UNION ALL
    
    -- Data quality metrics
    SELECT 
        4 as section_order,
        qm.category as section_name,
        NULL as layer,
        NULL as table_name,
        qm.DATA_SOURCE,
        qm.metric_name,
        TO_VARCHAR(qm.metric_value) as metric_value,
        CASE 
            WHEN qm.metric_percent IS NOT NULL THEN CONCAT(TO_VARCHAR(qm.metric_percent), '%')
            ELSE NULL
        END as metric_percent,
        NULL as oldest_record,
        NULL as newest_record
    FROM quality_metrics qm
    
    UNION ALL
    
    -- Risk summary
    SELECT 
        5 as section_order,
        rs.category as section_name,
        NULL as layer,
        NULL as table_name,
        rs.DATA_SOURCE,
        rs.metric_name,
        TO_VARCHAR(rs.metric_value) as metric_value,
        CASE 
            WHEN rs.metric_percent IS NOT NULL THEN CONCAT(TO_VARCHAR(rs.metric_percent), '%')
            ELSE NULL
        END as metric_percent,
        NULL as oldest_record,
        NULL as newest_record
    FROM risk_summary rs
)
ORDER BY section_order, DATA_SOURCE, layer, table_name, metric_name;
"""
    
    conn = None
    cursor = None
    
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        print("Creating monitoring view...")
        cursor.execute(monitoring_view_sql)
        print("Monitoring view created: CURATE.VW_RISK_ANALYSIS_PIPELINE_STATUS")
        
    except Exception as e:
        print(f"Error creating monitoring view: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    create_monitoring_view()