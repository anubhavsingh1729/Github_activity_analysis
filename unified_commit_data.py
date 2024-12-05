import pandas as pd
import os
import json
from config import UNIFIED_OUTPUT

def unify_commit_data(output_dir=UNIFIED_OUTPUT):
    """
    This function reads all JSON files in the `commit_data` directory, extracts relevant 
    commit information, and combines the data into a single Pandas DataFrame. The unified 
    DataFrame is saved as a Parquet file in the specified output directory.

    output_dir : str, optional
        The directory where the unified Parquet file will be saved. Defaults to the 
        value of `UNIFIED_OUTPUT` from the `config` module.

    """
    # List all files in the 'commit_data' directory
    files = os.listdir('commit_data')
    df = []  # Initialize an empty list to store DataFrames for each JSON file

    for f in files:
        # Process only JSON files
        if f.endswith('.json'): 
            file = os.path.join('commit_data', f)  # Construct the full path of the JSON file
            # Read and load JSON file line by line into a list of dictionaries
            with open(file, 'r') as f:
                data = [json.loads(line) for line in f]
                # Normalize JSON data into a Pandas DataFrame and append to the list
                df.append(pd.json_normalize(data))

    # Concatenate all DataFrames into a single DataFrame
    df = pd.concat(df)
    
    # Select and reorder relevant columns for the final DataFrame
    df = df[['origin', 'data.Author', 'data.AuthorDate', 'data.Commit', 
             'data.CommitDate', 'data.commit', 'data.files', 
             'data.message', 'data.Signed-off-by']]

    # Rename columns for better readability
    df = df.rename(columns={
        'data.Author': 'Author',
        'data.AuthorDate': 'AuthorDate',
        'data.Commit': 'Committer',
        'data.CommitDate': 'CommitDate',
        'data.commit': 'commit',
        'data.files': 'files',
        'data.message': 'CommitMessage',
        'data.Signed-off-by': 'Signed-off-by'
    })

    # Reset index for a clean DataFrame
    df = df.reset_index(drop=True)

    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Define the output file path and save the DataFrame as a Parquet file
    output_path = os.path.join(output_dir, "unified_df.parquet")
    df.to_parquet(output_path)
    
    # Print success message
    print("Commit Data of all Repositories aggregated into one DataFrame.")

# Call the function to unify commit data
#unify_commit_data()
    
