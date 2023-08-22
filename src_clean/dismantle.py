"""
PURPOSE:
    A script to run a dismantling analysis using the various lists of users.
        - We compare the following lists:
            - FIBers (naive)
            - Popular user (most followers)
            - Influential users (users who earned the most retweets)
            - Botscore users (users with highest bot scores)
    In this analysis we rank each user from the jan/feb time period based on
        one the four above values and then iteratively remove the highest ranking
        users. After each removal, we look at how many retweets of misinformation
        remain in the following eight months. This illustrates each users influence
        within the network, as well as the (positive) effect of removing
        them from the network.

INPUT:
    Project `config.ini` file with the following details included:
    ~~~~
    [VARS]

    [PATHS]
    DISMANTLING_DIR = /data_volume/super-spreaders/intermediate_files/dismantling

    [FILES]
    IFFYP_DISMANTLING_RESULTS = iffyp_dismantling_results.csv
    ~~~

OUTPUT:
    - A .csv which contains the dismantling results for each of the above
        ranking methods. Each column will represent one baseline.

Author: Matthew DeVerna
"""
### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Load packages ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
import os
import datetime

import numpy as np
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
    Conduct dismantling analysis, returning a list of proportions of misinformation
    remaining within the network.

    NOTE: Since Twitter attributes all retweets to the original poster, calculating
        the proportion of misinformation remaining for all users up to index ii
        is equivalent to calculating:
           --> (total_rts - sum_rts(`users_list[:ii+1]`)) / total_rts

    Parameters:
    -----------
    - users_list (list) : the pre-sorted list of user ids to remove from the network
    - total_rts (int/float) : the total retweets of misinformation in the network
    - rt_count_dict (dict) : dictionary where key = user id numbers and
        the values are lists of all their retweet counts

    Returns:
    -----------
    proportions (list): The proportions of misinformation remaining after dismantling.
        The value at proportions[ii] indicates the amount of misinformation remaining
        in the network after removing ii (ranked) users.
    """
    proportions = [1.0]

    # Note: This loop does not iterate through users one by one. It takes
    # one user, then two, then three... and so on.
    for ii in range(len(users_list)):
        user_num = ii + 1
        users = users_list[:user_num]

        total_rts_removed = get_rts_of(users, rt_count_dict)
        prop_rts_remaining = (total_rts - total_rts_removed) / total_rts

        proportions.append(prop_rts_remaining)
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
    fib_naive = l.load_iffyp_ss(all_users=True)
    baseline_bot = l.load_iffyp_tweet_bot_scores()
    baseline_pop = l.load_iffyp_baseline(name="popular")
    baseline_infl = l.load_iffyp_baseline(name="influential")
    print("Success.")

    # Since the get-FIB-scores.py script optimizes to ignore unneeded information,
    # many user ids are ignored and subsequently not given a fib-score. This means that their
    # FIB score should be zero. I find the missing user ids and assign them a score of zero
    print("Adding missing users to the FIB-naive frame...")
    missing_users = set(baseline_pop["original_tweeter_user_id"]).difference(
        set(fib_naive["user_id"])
    )
    missing_frame = pd.DataFrame(
        {"user_id": list(missing_users), "fib_score": np.repeat(0, len(missing_users))}
    )
    fib_naive = pd.concat([fib_naive, missing_frame], ignore_index=True)
    print("Success.")

    # Change the user_id column name where necessary
    print("Renaming baseline frame columns...")
    baseline_pop.rename(columns={"original_tweeter_user_id": "user_id"}, inplace=True)
    baseline_infl.rename(columns={"original_tweeter_user_id": "user_id"}, inplace=True)
    print("Success.")

    # Calculate mean botscore for only the baseline users
    baseline_bot = baseline_bot[
        baseline_bot["user_id"].isin(list(baseline_pop["user_id"]))
    ]
    baseline_bot = (
        baseline_bot.groupby("user_id")["bot_score_lite"].mean().reset_index()
    )

    # Ensure all frames are sorted
    print("Ensuring frames are sorted from highest to lowest...")
    fib_naive.sort_values(by="fib_score", ascending=False, inplace=True)
    baseline_bot.sort_values(by="bot_score_lite", ascending=False, inplace=True)
    baseline_pop.sort_values(
        by="original_tweeter_f_count", ascending=False, inplace=True
    )
    baseline_infl.sort_values(by="retweet_count", ascending=False, inplace=True)
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
        # If they exist already, add to the existing list
        if user in rt_count_dict:
            rt_count_dict[user].append(rt_count)
        # otherwise, initialize with first list
        else:
            rt_count_dict.update({user: [rt_count]})
    print("Success.")

    # Iterate through all frames, calculating the remaining misinfo proportion
    print("Begin dismantling procedure...")
    frames = [
        ("botscore", baseline_bot),
        ("fib-naive", fib_naive),
        ("pop", baseline_pop),
        ("infl", baseline_infl),
    ]

    total_rts = unique_mar_oct_data["retweet_count"].sum()

    data_dict = dict()
    for name, frame in frames:
        users_list = list(frame["user_id"])
        proportions = dismantle(users_list, total_rts, rt_count_dict)
        data_dict.update({name: proportions})
        print(f"Dismantling complete for: {name}")
    print("Success.")

    print("Creating output dataframe...")
    output_frame = pd.DataFrame(
        {
            "fib-naive": pd.Series(data_dict["fib-naive"]),
            "botscore": pd.Series(data_dict["botscore"]),
            "popular": pd.Series(data_dict["pop"]),
            "influential": pd.Series(data_dict["infl"]),
        }
    )

    # Create absolute output file path
    print("Writing dataframe to disk...")
    # Create absolute output file path
    output_dir = config["PATHS"]["DISMANTLING_DIR"]
    fname = config["FILES"]["IFFYP_DISMANTLING_RESULTS"]
    abs_output_path = os.path.join(output_dir, fname)

    # Write file
    output_frame.to_csv(abs_output_path, index=False)
    print("~~~ Script Complete ~~~")
