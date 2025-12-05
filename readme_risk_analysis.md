# Show help
python risk_analysis_cli.py

# Show summary statistics
python risk_analysis_cli.py --summary

# Show top 20 most risky repositories
python risk_analysis_cli.py --risky 20

# Show top 10 healthiest repositories
python risk_analysis_cli.py --healthy

# Show risk analysis by programming language
python risk_analysis_cli.py --languages

# Search for repositories containing "django"
python risk_analysis_cli.py --search django

# Get detailed report for specific repository
python risk_analysis_cli.py --report facebook/react

# Export all data to CSV
python risk_analysis_cli.py --export my_report.csv

# Run all reports
python risk_analysis_cli.py --all