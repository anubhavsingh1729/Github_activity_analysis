from github import Github
from config import GITHUB_TOKEN

def fetch_commits(repo_name):
    g=Github(GITHUB_TOKEN)
    repo = g.get_repo(repo_name)
    commits = repo.get_commits()

    commit_data = []

    for commit in commits:
        try:
            commit_details = {
                "sha": commit.sha,
                "author": commit.author.login if commit.author else None,
                "date": commit.commit.author.date.isoformat(),
                "message": commit.commit.message,
                "files": [
                    {
                        "filename": file.filename,
                        "additions": file.additions,
                        "deletions": file.deletions,
                        "changes": file.changes,
                        "patch": file.patch,
                    }
                    for file in commit.files
                ],
                "repo_name": repo_name,
            }
            commit_data.append(commit_details)
        except Exception as e:
            print(f"Error processing commit {commit.sha}: {e}")
    return commit_data

