import requests
import json
import time
import argparse
import logging
import os
from tqdm import tqdm

parser = argparse.ArgumentParser(description='Fetch PRs from specified GitHub repository.')
parser.add_argument('repository', choices=['lsb', 'asb'], help='Specify which repository to fetch from: lsb (LandSandBoat/server) or asb (AirSkyBoat/AirSkyBoat)')
args = parser.parse_args()
repo_map = {
    'lsb': ('LandSandBoat', 'server'),
    'asb': ('AirSkyBoat', 'AirSkyBoat')
}
selected_repo = repo_map[args.repository]

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('fetch_prs.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger()

# Configuration
TOKEN = ''
HEADERS = {
    'Authorization': f'token {TOKEN}',
    'User-Agent': 'PythonScript'
}

MAX_RETRIES = 3
PER_PAGE = 100
STATE_FILE = 'last_state.json'

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

OUTPUT_FOLDER = 'output'

if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)

def save_state(repo, last_commit_sha):
    state_data = {}
    # If the state file exists, read its current content
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            state_data = json.load(f)

    # Update or add the state for the given repository
    state_data[repo] = last_commit_sha

    # Write the entire state data back to the file
    with open(STATE_FILE, 'w') as f:
        json.dump(state_data, f)


def load_state(repo):
    try:
        # Read the entire state data from the file
        with open(STATE_FILE, 'r') as f:
            data = json.load(f)
            # Return the state for the given repository or None if not found
            return data.get(repo, None)
    except FileNotFoundError:
        return None
    

def delete_repo_state(repo_key):
    """Prompt the user to delete the last state for a specific repository if it exists."""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as file:
            state_data = json.load(file)
        if repo_key in state_data:
            # Only prompt if the key exists
            response = input(f"Do you want to delete the last state for the repository '{repo_key}'? (y/n): ").strip().lower()
            if response == 'y':
                del state_data[repo_key]
                with open(STATE_FILE, 'w') as file:
                    json.dump(state_data, file)
                logger.info(f"The last state for the repository '{repo_key}' has been deleted.")
            elif response == 'n':
                logger.info(f"The last state for the repository '{repo_key}' will not be deleted.")
            else:
                logger.warning("Invalid input. The last state for the repository will not be deleted.")
        else:
            logger.info(f"No state found for the repository '{repo_key}'.")
    else:
        logger.info("The last_state.json file does not exist.")


def get_commit_data(commit, owner, repo_name):
    sha = commit['sha']
    commit_url = f'https://github.com/{owner}/{repo_name}/commit/{sha}'
    return {'sha': sha, 'message': commit['commit']['message'], 'url': commit_url}


def fetch_pull_requests(owner, repo):
    prs = []
    last_commit_sha = load_state(repo)
    url = f'https://api.github.com/repos/{owner}/{repo}/pulls?state=closed&per_page={PER_PAGE}'
    progress_bar = tqdm(desc='Fetching PRs', unit='PR')

    while url:
        retries = 0
        success = False
        while retries < MAX_RETRIES and not success:
            response = SESSION.get(url)

            if response.status_code == 200:  # Success
                json_response = response.json()

                if 'last' in response.links and 'page' in response.links['last']:
                    total_pages = int(response.links['last']['url'].split('page=')[-1])
                    progress_bar.total = total_pages * PER_PAGE

                for pr in json_response:
                    if pr['merged_at']:
                        pr_info = {
                            'merged_at': pr['merged_at'],
                            'title': pr['title'],
                            'url': pr['html_url'],
                            'commits': []
                        }
                        # Fetch commits for this PR only once
                        commits_response = SESSION.get(pr['commits_url'])
                        if commits_response.status_code == 200:
                            all_commits = commits_response.json()

                            if last_commit_sha and any(c['sha'] == last_commit_sha for c in all_commits):
                                logger.info(f'Reached last known commit {last_commit_sha}. Stopping fetch.')
                                return prs

                            for commit in all_commits:
                                commit_data = get_commit_data(commit, owner, repo)
                                pr_info['commits'].append(commit_data)

                            # Add this PR's information to the list
                            prs.append(pr_info)

                            # Save the latest commit SHA
                            if all_commits:
                                save_state(repo, all_commits[0]['sha'])

                        progress_bar.update(1)

                # Check for rate limits
                remaining = int(response.headers.get('X-RateLimit-Remaining', 0))
                if remaining < 5:
                    reset_time = int(response.headers.get('X-RateLimit-Reset'))
                    sleep_duration = max(0, reset_time - time.time()) + 10
                    logger.warning(f'Approaching rate limit. Sleeping for {sleep_duration} seconds...')
                    time.sleep(sleep_duration)

                # Check for pagination in 'Link' header
                if 'next' in response.links:
                    url = response.links['next']['url']
                else:
                    url = None  # Stop the loop if there's no next page
                success = True

            elif response.status_code == 403:  # Possible rate limit exceeded
                logger.error('Rate limit exceeded. Retrying after a brief wait...')
                time.sleep(60)
                retries += 1
            else:
                logger.error(f'Error {response.status_code}: {response.text}. Retrying...')
                time.sleep(5)
                retries += 1

        if not success:
            logger.critical('Max retries reached. Stopping fetch process.')
            break

    return prs


def save_to_ndjson(prs, filename):
    """Save pull requests and their commits to an NDJSON file."""
    with open(os.path.join(OUTPUT_FOLDER, filename), 'w', encoding='utf-8') as f:
        for pr in prs:
            f.write(json.dumps(pr) + '\n')


def extract_shas_from_prs(prs):
    """Extract all SHAs from a list of pull requests."""
    return {commit['sha'] for pr in prs for commit in pr['commits']}


if __name__ == '__main__':
    logger.info('Script execution started.')

    # Prompt to delete the last processed commit from last_state.json file
    delete_repo_state(args.repository)

    # Fetching pull requests and extracting their commits
    prs = fetch_pull_requests(*selected_repo)
    shas = extract_shas_from_prs(prs)
    logger.info(f'Fetched {len(shas)} commits for {selected_repo[0]}.')

    # Save to JSON file
    filename = f'{args.repository}_prs.json'
    save_to_ndjson(prs, filename)
    logger.info(f'Successfully saved to {filename}.json.')

    logger.info('Script execution completed successfully.')
