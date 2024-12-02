# Fetch GitHub Commits

This project fetches commit data from specified GitHub repositories and saves the results to a designated output directory. 

---

## Configuration

Define a config.py file with the following settings:

- `GITHUB_TOKEN`: Your personal GitHub access token for authentication.
- `REPOSITORIES`: A list of GitHub repositories to fetch commit data from.
- `OUTPUT_PATH`: The local directory where fetched commit data will be saved.

### Example Configuration

```config.py
# Define configurations:

GITHUB_TOKEN = "your_personal_access_token"

REPOSITORIES = [
    "owner/repo1",
    "owner/repo2"
]

OUTPUT_PATH = "./path_to_output/"

UNIFIED_OUTPUT = "./path_to_unified_output/"

PERCEVAL_OUTPUT = "./path_to_perceval_commit_data/"
