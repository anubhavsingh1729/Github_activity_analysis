import pandas as pd
import os
import json
from config import UNIFIED_OUTPUT

def unify_commit_data(output_dir=UNIFIED_OUTPUT):
    files = os.listdir('commit_data')
    df = []
    for f in files:
        if f.endswith('.json'): 
            file = os.path.join('commit_data',f)
            with open(file, 'r') as f:
                data = [json.loads(line) for line in f]
                df.append(pd.json_normalize(data))
    df = pd.concat(df)
    df = df[['origin', 'data.Author', 'data.AuthorDate', 'data.Commit', 'data.CommitDate', 'data.commit','data.files', 
             'data.message','data.Signed-off-by']]

    df = df.rename(columns = {'data.Author':'Author','data.AuthorDate':'AuthorDate', 'data.Commit':'Committer', 
                              'data.CommitDate':'CommitDate', 'data.commit':'commit','data.files':'files', 
                              'data.message':'CommitMessage', 'data.Signed-off-by':'Signed-off-by'})
    df = df.reset_index(drop=True)

    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir,"unified_df.parquet")
    df.to_parquet(output_path)
    print("Commit Data of all Repositories aggregated into one DataFrame.")

unify_commit_data()
    
