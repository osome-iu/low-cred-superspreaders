"""
Purpose: Generate a file that contains all tweet IDs.

Author: Matthew DeVerna
"""
import os

import pandas as pd

from utils import Loader

CHUNKS = 1
MB_MULTIPLIER = 10**-6  # Multiply num bytes by this to get MB
RAW_DUMP_DIR = "raw"
MAX_MB = 100
DATA_DIR = "../data/tweet_ids"

if __name__ == "__main__":
    l = Loader()
    data = l.load_iffyp_tweets_jan_2_oct()

    print("Setting up output directory...")
    os.makedirs(DATA_DIR, exist_ok=True)

    print("Generate set of tweet_IDs...")
    tweet_ids = set(data["original_tweet_id"]).union(
        set(data.loc[data["rt_tweet_id"] != "n/a"]["rt_tweet_id"])
    )
    tweet_ids = pd.DataFrame(list(tweet_ids), columns=["tweet_id"])

    print("Saving file to parquet...")
    out_file_path = os.path.join(DATA_DIR, f"tweet_ids.parquet")
    tweet_ids.to_parquet(out_file_path, index=False, engine="pyarrow")
    print("--- Script complete. ---")
