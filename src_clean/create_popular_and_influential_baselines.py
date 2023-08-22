"""
Purpose:
    - This script creates files for two sets of baselines.
        1. "Popular" users - these are the users who have the most followers 
            from the Jan/Feb iffy+ data.
        2. "Influential" users - these are the users who received the most
            retweets during the Jan/Feb iffy+ data.

INPUT:
    Project `config.ini` file with the following details included:
    ~~~~
    [VARS]

    [PATHS]
    BASELINES_DIR = /data_volume/super-spreaders/intermediate_files/baselines

    [FILES]
    BASELINE_INFLUENTIAL = iffyp_influential_baseline.csv
    BASELINE_POPULAR = iffyp_popular_baseline.csv
    ~~~

Output:
    - Two .csv files where the columns represents an individual user id and
        the other represents the value for the baseline metric (number
        of retweets or number of followers). Each file will be sorted
        from largest to smallest.
    - Files:
        1. iffyp_influential_baseline.csv
        2. iffyp_popular_baseline.csv
"""
import datetime
import os

from utils import Loader
from utils import convert_twitter_strings_2_dates
from utils import parse_cl_args, parse_config_file

## Execute Script ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == "__main__":
    # Load the config file variables
    args = parse_cl_args()
    config = parse_config_file(args.config_file)

    # Initialize our Loader class and load the necessary data
    print("Loading data...")
    l = Loader()
    data = l.load_iffyp_tweets_jan_2_oct()
    print("Success.")

    # Convert string dates to datetime objects for wrangling
    print("Converting all date strings to datetime objects...")
    data["dt_obj"] = data["created_at"].map(convert_twitter_strings_2_dates)
    print("Success.")

    # Select only the jan/feb data
    print("Selecting only the jan/feb data...")
    jan_feb_data = data.loc[data["dt_obj"] < datetime.datetime(2020, 3, 1)]
    del data
    print("Success.")

    # Check no dates after February
    try:
        print("checking all tweets are before 2020/03/01...")
        assert jan_feb_data["dt_obj"].max() < datetime.datetime(2020, 3, 1)
        print("Success.")
    except Exception as e:
        raise Exception(
            "We have failed the date check assertion, investigate why"
            f" certain dates after 2020/03/01 were captured.\n\n{e}"
        )

    # Rank users by their total retweet counts
    try:
        print("Trying to build the influence frame...")
        # Take retweets with greatest retweet count and remove duplicates
        non_duplicated = (
            jan_feb_data.groupby(["original_tweet_id", "original_tweeter_user_id"])[
                "retweet_count"
            ]
            .max()
            .reset_index()
        )

        # Group by the original tweeter and add up all their retweet counts
        influence_frame = (
            non_duplicated.groupby(["original_tweeter_user_id"])["retweet_count"]
            .sum()
            .reset_index()
        )

        # Sort by RT count and then reset the indices for a clean df
        influence_frame.sort_values(by="retweet_count", ascending=False, inplace=True)
        influence_frame.reset_index(drop=True, inplace=True)
        print("Success")
    except Exception as e:
        raise Exception(f"Problem building influence frame. \n\n{e}")

    # Rank users by their number of followers
    try:
        print("Trying to build the popular frame...")

        # Calculate mean follower count
        popular_frame = (
            jan_feb_data.groupby(["original_tweeter_user_id"])[
                "original_tweeter_f_count"
            ]
            .mean()
            .reset_index()
        )

        # Sort by greatest number of followers
        popular_frame.sort_values(
            by="original_tweeter_f_count", ascending=False, inplace=True
        )

        # Clean up the index
        popular_frame = popular_frame.reset_index(drop=True)
        print("Success")
    except Exception as e:
        raise Exception(f"Problem building popular frame. \n\n{e}")

    # Create output filenames
    try:
        print("Trying to create output paths...")
        output_dir = config["PATHS"]["BASELINES_DIR"]
        influence_name = config["FILES"]["BASELINE_INFLUENTIAL"]
        popular_name = config["FILES"]["BASELINE_POPULAR"]
        inf_fname = os.path.join(output_dir, influence_name)
        pop_fname = os.path.join(output_dir, popular_name)
        print("Success")
    except Exception as e:
        raise Exception(f"Problem creating output paths. \n\n{e}")

    # Write data
    try:
        print("Trying to write data to the disk...")
        influence_frame.to_csv(inf_fname, index=False)
        popular_frame.to_csv(pop_fname, index=False)
    except Exception as e:
        raise Exception(f"Problem writing data to the disk. \n\n{e}")

    print("~~~ Script Complete ~~~")
