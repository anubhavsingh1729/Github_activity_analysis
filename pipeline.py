from get_commits import fetch_commits
from process_commits import process_commits,init_spark
from save import save_to_parquet
from perceval import get_perceval
from config import REPOSITORIES,OUTPUT_PATH
from unified_commit_data import unify_commit_data


def git_api():

    spark = init_spark()
    raw_data = []

    for repo in REPOSITORIES:
        print(f"fetching commit for {repo}...")
        commits = fetch_commits(repo)
        raw_data.extend(commits)

    print("Processing Data...")

    commit_df = process_commits(spark,raw_data)

    print("saving Dataframe...")
    save_to_parquet(commit_df,OUTPUT_PATH)

    print("pipeline complete")

def perceval_api():
    for repo in REPOSITORIES:
        get_perceval(repo)

def main():
    #git_api()
    perceval_api()
    unify_commit_data()

if __name__=="__main__":
    main()
