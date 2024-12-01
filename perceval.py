import perceval

import os
import subprocess

def fetch_commits(repo_url, output_file, token=None):
    """
    Fetches commits from a GitHub repository using Perceval and saves them to a JSON file.
    
    :param repo_url: URL of the GitHub repository.
    :param output_file: Path to save the JSON output.
    :param token: GitHub API token (optional for authenticated requests).
    """
    cmd = [
        "perceval",
        "git",
        repo_url,
        "--json-line"
    ]
    if token:
        cmd.extend(["--token", token])
    
    print(f"Running: {' '.join(cmd)}")
    with open(output_file, "w") as out:
        subprocess.run(cmd, stdout=out, check=True)
    print(f"Commits fetched and saved to {output_file}")

def get_perceval(repo_name,output_dir = "commit_data"):
    REPO_URL = f"https://github.com/{repo_name}.git"

    os.makedirs(output_dir, exist_ok=True)
    sanitized_repo_name = repo_name.replace("/", "_")
    OUTPUT_FILE = os.path.join(output_dir, f"{sanitized_repo_name}_commits.json")

    fetch_commits(REPO_URL, OUTPUT_FILE)