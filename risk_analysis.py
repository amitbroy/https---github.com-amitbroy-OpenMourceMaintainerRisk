import snowflake.connector
import os
from dotenv import load_dotenv
import pandas as pd
import argparse
from datetime import datetime
import sys

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

def show_summary():
    """Show summary statistics of risk analysis"""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # Get overall summary
        query = """
        SELECT 
            COUNT(*) as total_repos,
            ROUND(AVG(RISK_SCORE), 2) as avg_risk_score,
            COUNT(CASE WHEN RISK_CATEGORY = 'HIGH' THEN 1 END) as high_risk_count,
            COUNT(CASE WHEN RISK_CATEGORY = 'MEDIUM' THEN 1 END) as medium_risk_count,
            COUNT(CASE WHEN RISK_CATEGORY = 'LOW' THEN 1 END) as low_risk_count,
            MAX(LAST_UPDATED) as last_updated
        FROM CURATE.RISK_ANALYSIS_DATA_PRODUCT
        WHERE DATA_SOURCE = 'git_hub'
        """
        
        cursor.execute(query)
        result = cursor.fetchone()
        
        print("\n" + "="*70)
        print("RISK ANALYSIS SUMMARY")
        print("="*70)
        print(f"Total Repositories: {result[0]}")
        print(f"Average Risk Score: {result[1]}/100")
        print(f"\nRisk Distribution:")
        print(f"  High Risk:   {result[2]} ({result[2]/result[0]*100:.1f}%)")
        print(f"  Medium Risk: {result[3]} ({result[3]/result[0]*100:.1f}%)")
        print(f"  Low Risk:    {result[4]} ({result[4]/result[0]*100:.1f}%)")
        print(f"\nLast Updated: {result[5]}")
        print("="*70)
        
    finally:
        cursor.close()
        conn.close()

def show_top_risky(limit=10):
    """Show top N most risky repositories"""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        query = f"""
        SELECT 
            FULL_NAME,
            LANGUAGE,
            STARS,
            COMMITS_90D,
            ACTIVE_CONTRIBUTORS_90D,
            DAYS_SINCE_LAST_RELEASE,
            OPEN_ISSUES,
            RISK_SCORE,
            RISK_CATEGORY
        FROM CURATE.RISK_ANALYSIS_DATA_PRODUCT
        WHERE DATA_SOURCE = 'git_hub'
        ORDER BY RISK_SCORE DESC
        LIMIT {limit}
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        print("\n" + "="*100)
        print(f"TOP {limit} MOST RISKY REPOSITORIES")
        print("="*100)
        print(f"{'Repository':<40} {'Lang':<10} {'Stars':<8} {'Commits':<8} {'Contrib':<8} {'Last Rel':<10} {'Issues':<8} {'Score':<8} {'Risk'}")
        print("-"*100)
        
        for row in results:
            full_name = row[0][:38] + ".." if len(row[0]) > 40 else row[0]
            print(f"{full_name:<40} {row[1] or 'N/A':<10} {row[2]:<8} {row[3]:<8} {row[4]:<8} {row[5]:<10} {row[6]:<8} {row[7]:<8.1f} {row[8]}")
        
        print("="*100)
        
    finally:
        cursor.close()
        conn.close()

def show_healthiest(limit=10):
    """Show top N healthiest repositories"""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        query = f"""
        SELECT 
            FULL_NAME,
            LANGUAGE,
            STARS,
            COMMITS_90D,
            ACTIVE_CONTRIBUTORS_90D,
            DAYS_SINCE_LAST_RELEASE,
            OPEN_ISSUES,
            RISK_SCORE,
            RISK_CATEGORY
        FROM CURATE.RISK_ANALYSIS_DATA_PRODUCT
        WHERE DATA_SOURCE = 'git_hub'
        ORDER BY RISK_SCORE ASC
        LIMIT {limit}
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        print("\n" + "="*100)
        print(f"TOP {limit} HEALTHIEST REPOSITORIES (LOWEST RISK)")
        print("="*100)
        print(f"{'Repository':<40} {'Lang':<10} {'Stars':<8} {'Commits':<8} {'Contrib':<8} {'Last Rel':<10} {'Issues':<8} {'Score':<8} {'Risk'}")
        print("-"*100)
        
        for row in results:
            full_name = row[0][:38] + ".." if len(row[0]) > 40 else row[0]
            print(f"{full_name:<40} {row[1] or 'N/A':<10} {row[2]:<8} {row[3]:<8} {row[4]:<8} {row[5]:<10} {row[6]:<8} {row[7]:<8.1f} {row[8]}")
        
        print("="*100)
        
    finally:
        cursor.close()
        conn.close()

def show_by_language():
    """Show risk analysis by programming language"""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        query = """
        SELECT 
            COALESCE(LANGUAGE, 'Unknown') as language,
            COUNT(*) as repo_count,
            ROUND(AVG(RISK_SCORE), 2) as avg_risk_score,
            COUNT(CASE WHEN RISK_CATEGORY = 'HIGH' THEN 1 END) as high_risk,
            COUNT(CASE WHEN RISK_CATEGORY = 'MEDIUM' THEN 1 END) as medium_risk,
            COUNT(CASE WHEN RISK_CATEGORY = 'LOW' THEN 1 END) as low_risk
        FROM CURATE.RISK_ANALYSIS_DATA_PRODUCT
        WHERE DATA_SOURCE = 'git_hub'
        GROUP BY LANGUAGE
        ORDER BY repo_count DESC
        LIMIT 15
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        print("\n" + "="*90)
        print("RISK ANALYSIS BY PROGRAMMING LANGUAGE")
        print("="*90)
        print(f"{'Language':<15} {'Repos':<8} {'Avg Score':<10} {'High':<8} {'Medium':<8} {'Low':<8}")
        print("-"*90)
        
        for row in results:
            print(f"{row[0][:14]:<15} {row[1]:<8} {row[2]:<10.1f} {row[3]:<8} {row[4]:<8} {row[5]:<8}")
        
        print("="*90)
        
    finally:
        cursor.close()
        conn.close()

def search_repository(search_term):
    """Search for specific repository"""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        query = """
        SELECT 
            FULL_NAME,
            LANGUAGE,
            STARS,
            COMMITS_90D,
            ACTIVE_CONTRIBUTORS_90D,
            DAYS_SINCE_LAST_RELEASE,
            OPEN_ISSUES,
            RISK_SCORE,
            RISK_CATEGORY,
            LAST_UPDATED
        FROM CURATE.RISK_ANALYSIS_DATA_PRODUCT
        WHERE DATA_SOURCE = 'git_hub'
          AND UPPER(FULL_NAME) LIKE UPPER(%s)
        ORDER BY RISK_SCORE DESC
        LIMIT 20
        """
        
        cursor.execute(query, (f'%{search_term}%',))
        results = cursor.fetchall()
        
        if not results:
            print(f"\nNo repositories found matching: '{search_term}'")
            return
        
        print(f"\n" + "="*120)
        print(f"SEARCH RESULTS FOR: '{search_term}' ({len(results)} repositories found)")
        print("="*120)
        
        for row in results:
            print(f"\nRepository: {row[0]}")
            print(f"Language: {row[1] or 'N/A'}")
            print(f"Stars: {row[2]:,} | Recent Commits: {row[3]} | Active Contributors: {row[4]}")
            print(f"Days since last release: {row[5]} | Open Issues: {row[6]}")
            print(f"Risk Score: {row[7]:.1f}/100 ({row[8]} RISK)")
            print(f"Last Analyzed: {row[9]}")
            print("-"*60)
        
        print("="*120)
        
    finally:
        cursor.close()
        conn.close()

def export_to_csv(filename="risk_analysis_export.csv"):
    """Export risk analysis data to CSV"""
    conn = get_connection()
    
    try:
        query = """
        SELECT 
            DATA_SOURCE,
            FULL_NAME,
            LANGUAGE,
            STARS,
            COMMITS_90D,
            ACTIVE_CONTRIBUTORS_90D,
            DAYS_SINCE_LAST_RELEASE,
            OPEN_ISSUES,
            RISK_SCORE,
            RISK_CATEGORY,
            LAST_UPDATED
        FROM CURATE.RISK_ANALYSIS_DATA_PRODUCT
        WHERE DATA_SOURCE = 'git_hub'
        ORDER BY RISK_SCORE DESC
        """
        
        df = pd.read_sql(query, conn)
        df.to_csv(filename, index=False)
        
        print(f"\n✓ Exported {len(df)} records to '{filename}'")
        print(f"  Columns exported: {', '.join(df.columns)}")
        
        # Show quick summary of exported data
        print(f"\n  Export Summary:")
        print(f"  - Total records: {len(df)}")
        print(f"  - Average risk score: {df['RISK_SCORE'].mean():.1f}")
        print(f"  - High risk: {(df['RISK_CATEGORY'] == 'HIGH').sum()}")
        print(f"  - Medium risk: {(df['RISK_CATEGORY'] == 'MEDIUM').sum()}")
        print(f"  - Low risk: {(df['RISK_CATEGORY'] == 'LOW').sum()}")
        
    finally:
        conn.close()

def show_detailed_report(repo_name):
    """Show detailed risk analysis for a specific repository"""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        query = """
        SELECT 
            FULL_NAME,
            LANGUAGE,
            STARS,
            COMMITS_90D,
            ACTIVE_CONTRIBUTORS_90D,
            DAYS_SINCE_LAST_RELEASE,
            OPEN_ISSUES,
            RISK_SCORE,
            RISK_CATEGORY,
            LAST_UPDATED
        FROM CURATE.RISK_ANALYSIS_DATA_PRODUCT
        WHERE DATA_SOURCE = 'git_hub'
          AND UPPER(FULL_NAME) = UPPER(%s)
        """
        
        cursor.execute(query, (repo_name,))
        result = cursor.fetchone()
        
        if not result:
            print(f"\nRepository not found: '{repo_name}'")
            print("Try searching with a partial name using: python risk_analysis_cli.py --search <term>")
            return
        
        print("\n" + "="*70)
        print("DETAILED RISK ANALYSIS REPORT")
        print("="*70)
        print(f"Repository: {result[0]}")
        print(f"Language: {result[1] or 'N/A'}")
        print(f"Last Updated: {result[9]}")
        print("\n" + "-"*70)
        print("METRICS:")
        print(f"  • Stars: {result[2]:,}")
        print(f"  • Commits (last 90 days): {result[3]}")
        print(f"  • Active Contributors (last 90 days): {result[4]}")
        print(f"  • Days since last release: {result[5]}")
        print(f"  • Open Issues: {result[6]}")
        print("\n" + "-"*70)
        print("RISK ASSESSMENT:")
        print(f"  • Risk Score: {result[7]:.1f}/100")
        print(f"  • Risk Category: {result[8]}")
        print("\n" + "-"*70)
        print("INTERPRETATION:")
        
        if result[8] == 'HIGH':
            print("  ⚠️  HIGH RISK: This repository shows significant risk factors.")
            print("     Consider finding alternatives or closely monitoring usage.")
        elif result[8] == 'MEDIUM':
            print("  ⚠️  MEDIUM RISK: Some concerns identified.")
            print("     Monitor regularly and have contingency plans.")
        else:
            print("  ✅ LOW RISK: Repository appears healthy and well-maintained.")
            print("     Suitable for production use with standard monitoring.")
        
        print("="*70)
        
    finally:
        cursor.close()
        conn.close()

def main():
    parser = argparse.ArgumentParser(description='GitHub Repository Risk Analysis CLI')
    parser.add_argument('--summary', action='store_true', help='Show summary statistics')
    parser.add_argument('--risky', type=int, nargs='?', const=10, help='Show top N risky repositories (default: 10)')
    parser.add_argument('--healthy', type=int, nargs='?', const=10, help='Show top N healthiest repositories (default: 10)')
    parser.add_argument('--languages', action='store_true', help='Show risk analysis by programming language')
    parser.add_argument('--search', type=str, help='Search for repositories by name')
    parser.add_argument('--export', type=str, nargs='?', const='risk_analysis.csv', help='Export data to CSV file')
    parser.add_argument('--report', type=str, help='Show detailed report for specific repository')
    parser.add_argument('--all', action='store_true', help='Run all reports (summary, languages, top risky/healthy)')
    
    args = parser.parse_args()
    
    # If no arguments provided, show help
    if len(sys.argv) == 1:
        parser.print_help()
        print("\nExamples:")
        print("  python risk_analysis_cli.py --summary")
        print("  python risk_analysis_cli.py --risky 20")
        print("  python risk_analysis_cli.py --search django")
        print("  python risk_analysis_cli.py --report facebook/react")
        print("  python risk_analysis_cli.py --all")
        return
    
    try:
        print(f"\n📊 GitHub Repository Risk Analysis")
        print(f"   Data Source: git_hub | Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"   " + "-"*50)
        
        if args.all:
            show_summary()
            show_by_language()
            show_top_risky(15)
            show_healthiest(15)
        else:
            if args.summary:
                show_summary()
            
            if args.risky:
                show_top_risky(args.risky)
            
            if args.healthy:
                show_healthiest(args.healthy)
            
            if args.languages:
                show_by_language()
            
            if args.search:
                search_repository(args.search)
            
            if args.export:
                export_to_csv(args.export)
            
            if args.report:
                show_detailed_report(args.report)
                
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("Check your Snowflake connection and ensure the tables exist.")
        sys.exit(1)

if __name__ == "__main__":
    main()