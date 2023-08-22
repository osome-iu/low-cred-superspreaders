"""
PURPOSE:
    A script to run the "gold standard" dismantling analysis.

    In this analysis we simply remove all users in the Jan/Feb
        time period from the future period, calculating how
        much misinformation each user removes from the future
        network. We sort them based on who removed the most
        misinformation and then save a file with a user and
        the proportion of misinformation they removed in each row.

INPUT:
    Project `config.ini` file with the following details included:
    ~~~~
    [VARS]

    [PATHS]
    DISMANTLING_DIR = /data_volume/super-spreaders/intermediate_files/dismantling

    [FILES]
    IFFYP_DISMANTLING_GOLD_STANDARD = iffyp_dismantling_results_gold_standard.csv
    ~~~

OUTPUT:
    - A .csv which contains, on each line, the proportion of misinformation
        removed by each user and that users ID number.

Author: Matthew DeVerna
"""
### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Load packages ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
import os
import datetime

import pandas as pd

from utils import Loader
from utils import parse_config_file, parse_cl_args, convert_twitter_strings_2_dates


### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Create functions ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def get_rts_of(user_ids=list, data=dict):
    """
    Get the sum of all retweets_counts OF all user ids in
    a list of user_ids.

    Parameters:
    -----------
    - user_ids (list) : user id of the account of interest
    - data (dict) : dictionary where key = user_id and
        the values = lists of user_ids retweet counts

    Returns:
    -----------
    Integer which is the sum of all retweet counts earned by that list of user ids
    """
    total_count = 0
    for user in user_ids:
        if user in data:
            sum_user_counts = sum(data[user])
        else:
            sum_user_counts = 0
        total_count += sum_user_counts
    return total_count


def dismantle(users_list, total_rts, rt_count_dict):
    """
    Dismantle `data`'s retweet counts based on all users within `users_frame`.
    Specifically, we return a list of proportions that represents the
    proportion of misinformation retweets REMOVED from `data`. Note that this function
    is slightly different from the one used in src_clean/dismantle.py which returns
    the amount of misinformation REMAINING.

    Parameters:
    -----------
    - users_list (list) : the pre-sorted list of user ids to remove from the network
    - total_rts (int/float) : the total retweets of misinformation in the network
    - rt_count_dict (dict) : dictionary where key = user id numbers and
        the values are lists of all their retweet counts

    Returns:
    -----------
    proportions (list of tuples): The proportion of misinformation removed by `user_id`.
        - Example: [(user_id1, proportion_removed), (user_id2, proportion_removed), ...]
    """
    proportions = list()

    for user_num, user in enumerate(users_list, start=1):
        total_rts_removed = get_rts_of([user], rt_count_dict)
        prop_rts_removed = total_rts_removed / total_rts

        proportions.append((user, prop_rts_removed))
        if (user_num % 2000) == 0:
            print(f"{user_num} users processed...")

    return proportions


### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Execute script ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == "__main__":
    # Load the config file variables
    args = parse_cl_args()
    config = parse_config_file(args.config_file)

    # Initialize our Loader class and load the needed data
    print("Loading data...")
    l = Loader()
    data = l.load_iffyp_tweets_jan_2_oct()

    # Doesn't matter which baseline we use because the users are the same
    baseline_pop = l.load_iffyp_baseline(name="popular")
    print("Success.")

    # Change the user_id column name
    print("Renaming baseline frame columns...")
    baseline_pop.rename(columns={"original_tweeter_user_id": "user_id"}, inplace=True)
    print("Success.")

    # Select only future months data
    print("Selecting only future data and removing unneeded data...")
    data["dt_obj"] = data["created_at"].map(convert_twitter_strings_2_dates)
    mar_oct_data = data.loc[data["dt_obj"] >= datetime.datetime(2020, 3, 1)]

    # Delete full frame and remove unneeded columns
    del data
    mar_oct_data = mar_oct_data[
        [
            "original_tweet_id",
            "original_tweeter_user_id",
            "retweet_count",
            "retweeting_user_id",
        ]
    ]
    print("Success.")

    print("Building data dictionaries...")

    # Create frame with only the greatest retweet counts we have on record (removing duplicates)
    unique_mar_oct_data = (
        mar_oct_data.groupby(["original_tweet_id", "original_tweeter_user_id"])[
            "retweet_count"
        ]
        .max()
        .reset_index()
    )

    # Create dictionary where key = user_id and values = lists of all their retweet counts
    u_id_rt_count_zipper = zip(
        unique_mar_oct_data["original_tweeter_user_id"],
        unique_mar_oct_data["retweet_count"],
    )
    rt_count_dict = {}
    for user, rt_count in u_id_rt_count_zipper:
        if user in rt_count_dict:
            rt_count_dict[user].append(rt_count)
        else:
            rt_count_dict.update({user: [rt_count]})
    print("Success.")

    # Iterate through all users, calculating the remaining misinfo proportion
    print("Begin dismantling procedure...")

    total_rts = unique_mar_oct_data["retweet_count"].sum()

    results = dismantle(
        users_list=list(baseline_pop["user_id"]),
        total_rts=total_rts,
        rt_count_dict=rt_count_dict,
    )

    print("Creating output dataframe...")
    output_frame = pd.DataFrame(results, columns=["user_ids", "prop_rts_remaining"])
    output_frame.sort_values(by="prop_rts_remaining", ascending=False, inplace=True)

    # Create absolute output file path
    print("Writing dataframe to disk...")
    # Create absolute output file path
    # output_dir = config["PATHS"]["DISMANTLING_DIR"]
    output_dir = config["PATHS"]["DISMANTLING_DIR_TEMP"]
    fname = config["FILES"]["IFFYP_DISMANTLING_GOLD_STANDARD"]
    abs_output_path = os.path.join(output_dir, fname)

    # Write file
    output_frame.to_csv(abs_output_path, index=False)
    print("~~~ Script Complete ~~~")
