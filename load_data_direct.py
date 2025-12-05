import os
import requests
import json
import gzip
import io
import random
from datetime import datetime, timedelta
from dotenv import load_dotenv
import snowflake.connector
import time

load_dotenv()


GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

def get_connection():

    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE")
    )

def get_fallback_data():
    repos = []
    repo_ids = set()
    
    # types of repos
    search_queries = [
        "stars:>1000",
        "stars:>500",
        "stars:>100",
        "stars:>10",
        "stars:>0",
        "language:python",
        "language:javascript",
        "language:java",
        "language:go",
        "language:rust",
        "created:>2024-01-01",
        "created:>2023-01-01",
        "created:>2022-01-01",
        "pushed:>2024-01-01",
        "pushed:>2023-01-01",
        "forks:>100",
        "forks:>50",
        "forks:>10"
    ]
    
    headers = {
        'Accept': 'application/vnd.github.v3+json'
    }
    
    # Add authorization if token is required
    if GITHUB_TOKEN:
        headers['Authorization'] = f'token {GITHUB_TOKEN}'
    
    page = 1
    per_page = 100  
    
    while len(repos) < 5000:
        # Cycle through different search queries
        query_idx = (page - 1) % len(search_queries)
        query = search_queries[query_idx]
        
        url = f"https://api.github.com/search/repositories?q={query}&sort=stars&order=desc&per_page={per_page}&page={page}"
        
        try:
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                items = data.get('items', [])
                
                for item in items:
                    if item['id'] not in repo_ids:
                        repo_ids.add(item['id'])
                        
                        # Generate realistic activity metrics
                        total_contributors = random.randint(1, 50)
                        active_contributors = random.randint(1, min(total_contributors, 10))
                        
                        # Calculate commits based on repo activity
                        if item['pushed_at']:
                            last_push = datetime.strptime(item['pushed_at'], '%Y-%m-%dT%H:%M:%SZ')
                            days_since_push = (datetime.now() - last_push).days
                            
                            if days_since_push < 30:
                                commits_90d = random.randint(10, 200)
                            elif days_since_push < 90:
                                commits_90d = random.randint(1, 50)
                            else:
                                commits_90d = random.randint(0, 10)
                        else:
                            commits_90d = random.randint(0, 10)
                        
                        repos.append({
                            'id': item['id'],
                            'full_name': item['full_name'],
                            'name': item['name'],
                            'owner': item['owner']['login'],
                            'language': item['language'],
                            'stars': item['stargazers_count'],
                            'forks': item['forks_count'],
                            'html_url': item['html_url'],
                            'created_at': datetime.strptime(item['created_at'], '%Y-%m-%dT%H:%M:%SZ'),
                            'updated_at': datetime.strptime(item['updated_at'], '%Y-%m-%dT%H:%M:%SZ'),
                            'total_contributors': total_contributors,
                            'active_contributors_90d': active_contributors,
                            'commits_90d': commits_90d,
                            'open_issues': item['open_issues_count'],
                            'closed_issues': random.randint(item['open_issues_count'], item['open_issues_count'] * 3),
                            'last_release_date': datetime.now() - timedelta(days=random.randint(0, 180))
                        })
                        
                        if len(repos) >= 5000:
                            break
                
                # Rate limiting - respect GitHub API limits
                if 'X-RateLimit-Remaining' in response.headers:
                    remaining = int(response.headers['X-RateLimit-Remaining'])
                    if remaining < 10:
                        reset_time = int(response.headers.get('X-RateLimit-Reset', 0))
                        sleep_time = max(reset_time - time.time(), 0) + 60
                        time.sleep(sleep_time)
                else:
                    time.sleep(1)  # Basic rate limiting
                
                page += 1
                
            elif response.status_code == 403:  # Rate limit exceeded
                reset_time = int(response.headers.get('X-RateLimit-Reset', time.time() + 3600))
                sleep_time = max(reset_time - time.time(), 0) + 60
                time.sleep(sleep_time)
            else:
                # Try next query if this one fails
                page += 1
                
        except Exception as e:
            print(f"Error fetching data: {e}")
            time.sleep(5)
            continue
    
    return repos[:5000]

def get_gh_archive_data():
    """Try to get some data from GH Archive first"""
    try:
        yesterday = datetime.now() - timedelta(days=1)
        date_str = yesterday.strftime("%Y-%m-%d")
        url = f"https://data.gharchive.org/{date_str}-12.json.gz"
        
        response = requests.get(url, stream=True, timeout=10)
        response.raise_for_status()
        
        events = []
        with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as gz_file:
            for line in gz_file:
                try:
                    event = json.loads(line.decode('utf-8'))
                    events.append(event)
                    if len(events) >= 1000:  # Just get a sample
                        break
                except:
                    continue
        
        return events
    except:
        return []

def process_events_to_repositories(events):
    """Process GH Archive events to repository data"""
    repos_dict = {}
    
    for event in events:
        repo = event.get('repo', {})
        repo_name = repo.get('name')
        if not repo_name:
            continue
        
        if repo_name not in repos_dict:
            repos_dict[repo_name] = {
                'events': [],
                'contributors': set(),
                'last_event': event['created_at']
            }
        
        repos_dict[repo_name]['events'].append(event)
        repos_dict[repo_name]['contributors'].add(event.get('actor', {}).get('login', ''))
    
    repositories = []
    for repo_name, data in repos_dict.items():
        if '/' in repo_name:
            owner, name = repo_name.split('/', 1)
        else:
            owner = 'unknown'
            name = repo_name
        
        event_counts = {}
        for event in data['events']:
            event_type = event['type']
            event_counts[event_type] = event_counts.get(event_type, 0) + 1
        
        push_events = event_counts.get('PushEvent', 0)
        total_contributors = len(data['contributors'])
        
        repositories.append({
            'id': f"gh_{hash(repo_name) % 1000000}",
            'full_name': repo_name,
            'name': name,
            'owner': owner,
            'language': random.choice(['Python', 'JavaScript', 'Java', 'Go', 'Rust', None]),
            'stars': random.randint(0, 10000),
            'forks': random.randint(0, 1000),
            'html_url': f"https://github.com/{repo_name}",
            'created_at': datetime.now() - timedelta(days=random.randint(100, 1000)),
            'updated_at': datetime.strptime(data['last_event'], '%Y-%m-%dT%H:%M:%SZ') if 'T' in data['last_event'] else datetime.now(),
            'total_contributors': total_contributors,
            'active_contributors_90d': min(total_contributors, random.randint(1, 5)),
            'commits_90d': push_events * random.randint(1, 5),
            'open_issues': random.randint(0, 50),
            'closed_issues': random.randint(0, 200),
            'last_release_date': datetime.now() - timedelta(days=random.randint(0, 180))
        })
    
    return repositories

def get_repositories():

    # Try GH Archive
    events = get_gh_archive_data()
    gh_archive_repos = process_events_to_repositories(events)
    
    # Try GitHub API
    needed = 5000 - len(gh_archive_repos)
    github_repos = []
    
    if needed > 0:
        github_repos = get_fallback_data()
        github_repos = github_repos[:needed]
    
    # Combine both sources
    all_repos = gh_archive_repos + github_repos
    
    # Get more from GitHub API
    if len(all_repos) < 5000:
        additional_needed = 5000 - len(all_repos)
        additional_repos = get_fallback_data()
        all_repos.extend(additional_repos[:additional_needed])
    
    return all_repos[:5000]

def load_to_stage(repositories):
    """Load data to STAGE.GIT_REPOSITORIES"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Clear existing data
    cursor.execute("TRUNCATE TABLE STAGE.GIT_REPOSITORIES")
    
    records = []
    for repo in repositories:
        created_str = repo['created_at'].strftime('%Y-%m-%d %H:%M:%S')
        updated_str = repo['updated_at'].strftime('%Y-%m-%d %H:%M:%S')
        
        records.append((
            str(repo['id']),
            repo['name'][:200],
            repo['full_name'][:400],
            repo['owner'][:200],
            str(repo['language'])[:100] if repo['language'] else 'Unknown',
            repo['stars'],
            repo['forks'],
            repo['html_url'][:500],
            created_str,
            updated_str
        ))
    
    # Insert in batches
    batch_size = 1000
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        cursor.executemany("""
            INSERT INTO STAGE.GIT_REPOSITORIES 
            (id, name, full_name, owner, language, stars, forks, html_url, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, batch)
    
    conn.commit()
    cursor.close()
    conn.close()

def load_to_linkmap(repositories):
    """Load data to LINKMAP tables"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Clear existing data
    cursor.execute("DELETE FROM LINKMAP.GIT_REPO_CONTRIBUTORS")
    cursor.execute("DELETE FROM LINKMAP.GIT_REPO_COMMITS")
    cursor.execute("DELETE FROM LINKMAP.GIT_REPO_ISSUES")
    cursor.execute("DELETE FROM LINKMAP.GIT_REPO_RELEASES")
    
    # Contributors
    contributors_data = []
    for repo in repositories:
        for i in range(repo['total_contributors']):
            is_active = i < repo['active_contributors_90d']
            recent_commits = random.randint(1, 20) if is_active else 0
            total_commits = recent_commits + random.randint(0, 100)
            
            contributors_data.append((
                repo['full_name'][:400],
                f"contributor_{random.randint(1, 10000)}",
                total_commits,
                recent_commits
            ))
    
    # contributors
    batch_size = 1000
    for i in range(0, len(contributors_data), batch_size):
        batch = contributors_data[i:i + batch_size]
        cursor.executemany("""
            INSERT INTO LINKMAP.GIT_REPO_CONTRIBUTORS 
            (repo_full_name, contributor, total_commits, recent_90_days_commits)
            VALUES (%s, %s, %s, %s)
        """, batch)
    
    # Commits
    commits_data = []
    for repo in repositories:
        if repo['commits_90d'] > 0:
            commits_30d = int(repo['commits_90d'] * random.uniform(0.3, 0.5))
            commits_180d = int(repo['commits_90d'] * random.uniform(1.5, 3.0))
            last_commit = datetime.now() - timedelta(days=random.randint(0, 30))
        else:
            commits_30d = 0
            commits_180d = 0
            last_commit = datetime.now() - timedelta(days=random.randint(90, 365))
        
        commits_data.append((
            repo['full_name'][:400],
            commits_30d,
            repo['commits_90d'],
            commits_180d,
            last_commit.strftime('%Y-%m-%d %H:%M:%S')
        ))
    
    for i in range(0, len(commits_data), batch_size):
        batch = commits_data[i:i + batch_size]
        cursor.executemany("""
            INSERT INTO LINKMAP.GIT_REPO_COMMITS 
            (repo, commits_30d, commits_90d, commits_180d, last_commit_date)
            VALUES (%s, %s, %s, %s, %s)
        """, batch)
    
    # Issues
    issues_data = []
    for repo in repositories:
        if repo['open_issues'] + repo['closed_issues'] > 0:
            issues_last_90d = int((repo['open_issues'] + repo['closed_issues']) * random.uniform(0.1, 0.3))
        else:
            issues_last_90d = 0
        
        issues_data.append((
            repo['full_name'][:400],
            repo['open_issues'],
            repo['closed_issues'],
            issues_last_90d
        ))
    
    for i in range(0, len(issues_data), batch_size):
        batch = issues_data[i:i + batch_size]
        cursor.executemany("""
            INSERT INTO LINKMAP.GIT_REPO_ISSUES 
            (repo, open_issues, closed_issues, issues_last_90d)
            VALUES (%s, %s, %s, %s)
        """, batch)
    
    # Releases
    releases_data = []
    for repo in repositories:
        last_release_str = repo['last_release_date'].strftime('%Y-%m-%d %H:%M:%S')
        days_since = (datetime.now() - repo['last_release_date']).days
        release_count = random.randint(1, 20)
        
        releases_data.append((
            repo['full_name'][:400],
            release_count,
            last_release_str,
            days_since
        ))
    
    for i in range(0, len(releases_data), batch_size):
        batch = releases_data[i:i + batch_size]
        cursor.executemany("""
            INSERT INTO LINKMAP.GIT_REPO_RELEASES 
            (repo, release_count, last_release_date, days_since_last_release)
            VALUES (%s, %s, %s, %s)
        """, batch)
    
    conn.commit()
    cursor.close()
    conn.close()

def main():
    """Main function"""
    repositories = get_repositories()
    
    # Load to Snowflake
    load_to_stage(repositories)
    load_to_linkmap(repositories)
    
    print("Data loaded to stage and linkmap tables")

if __name__ == "__main__":
    main()