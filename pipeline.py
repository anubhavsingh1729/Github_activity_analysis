from get_commits import fetch_commits
from process_commits import process_commits,init_spark
from save import save_to_parquet
from config import REPOSITORIES,OUTPUT_PATH


def main():

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

if __name__=="__main__":
    main()
