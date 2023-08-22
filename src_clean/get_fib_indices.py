#!/usr/bin/env python3

"""
IDENTIFYING SUPERSPREADERS

Purpose:
Calculate FIB indices of users within a set of tweet files. All tweet files should
contain only misinformation-containing tweets.

Note: The FIB-index is a repurposing of the h-index to specifically calculate
a social media user's influence within a misinformation network.
    - See the paper here: https://arxiv.org/abs/2207.09524

Input:
Call the script with the `-h` flag to get input/flag details or
see the argparse section of this code which details this
information as well.

Output:
- A .csv file where each line represents a single Twitter user and their 
calculated `FIB-index`.
- These users will be those identified as `superspreaders` of misinformation
based on the specified threshold.
- Filename style: "top-fibers{todays-date}--thresh-{threshold}.csv"

Author: Matthew R. DeVerna
"""

### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Load Packages ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
import argparse
import json

import datetime as dt
import numpy as np
import pandas as pd


### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Set Functions ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def parse_cl_args():
    """Read Command Line Arguments."""
    print("Parsing command line arguments...")

    # Initiate the parser
    parser = argparse.ArgumentParser(
        description="Select super-spreaders of misinformation from Moes Tavern output (2020, Twitter V1) based on provided percentile threshold."
    )

    # Add long and short argument
    parser.add_argument(
        "-d",
        "--data",
        metavar="Data",
        nargs="+",
        help="\nFilename(s) for the files you would like to parse. Navigate to directory of data, or pass full path. (Direct output from Moe's Tavern - new-line delimited json)",
        required=True,
    )
    parser.add_argument(
        "-t",
        "--threshold",
        metavar="Threshold",
        type=float,
        help="Percentile above which you'd like to select superspreaders. Float - Must be greater than 0 and less than or equal to 100. (Default = 99)",
        default=99,
    )

    # Read parsed arguments from the command line into "args"
    args = parser.parse_args()

    # Set data file list and threshold
    data_files = args.data
    threshold = args.threshold

    return data_files, threshold


def get_fib_scores(rt_counts):
    """
    Calculate the FIB-index.

    INPUT:
    - rt_counts (list) : list of retweet count values for all retweets sent by a single user.

    OUTPUT:
    - fib_position (int) : FIB index value, for the user who earned the retweet counts in `rt_coutns`.
    """

    rt_counts.sort()

    # The "[::-1]" below makes this for-loop iterate from the last number instead of starting at zero
    for fib_position in range(1, len(rt_counts) + 1)[::-1]:
        if rt_counts[-fib_position] >= fib_position:
            return fib_position

    # If the above criteria is never met, we return the fib_position as zero
    fib_position = 0
    return fib_position


def load_tweets(data_files):
    """Load Tweets"""

    # Create an empty dictionary to update with tweet objects (dictionary objects)
    tweets = {}

    # Create a counter which will eventually serve as the pandas dataframe index
    t_count = 0

    try:
        # Open each file
        for file in data_files:
            print(f"Loading tweets from file: {file}")

            with open(file, "r", encoding="utf-8") as f:
                # Iterate through each line
                for line in f:
                    tweet = json.loads(line)  # Read the json object as a dictionary

                    # Take only the important features of the retweeted_status object
                    if "retweeted_status" in tweet:
                        info = {
                            "user_id_str": tweet.get("retweeted_status").get(
                                "user_id_str"
                            ),
                            "id_str": tweet.get("retweeted_status").get("id_str"),
                            "retweet_count": tweet.get("retweeted_status").get(
                                "retweet_count"
                            ),
                        }
                        tweets.update(
                            {t_count: info}
                        )  # Update the tweets dictionary with that data
                        t_count += 1  # Increase the tweet index counter

                    # Take the base-level tweet if it's retweet count is greater than 0
                    # and there is no retweeted_status
                    elif ("retweeted_status" not in tweet) and (
                        tweet["retweet_count"] > 0
                    ):
                        info = {
                            "user_id_str": tweet["user_id_str"],
                            "id_str": tweet["id_str"],
                            "retweet_count": tweet["retweet_count"],
                        }
                        tweets.update(
                            {t_count: info}
                        )  # Update the tweets dictionary with that data
                        t_count += 1  # Increase the tweet index counter

        print(f"Total Tweets Compiled = {len(tweets.keys())}")

        return tweets

    # Raise this error if something weird happens loading the data
    except:
        raise Exception(
            f"""\n\n\nThere has been an unexpected error loading the tweets. One of three things have likely occured:

            1) You have not provided data from Moe's Tavern.
            2) The data from Moe's you've provided has been altered in some way by you.
            3) The data is now output by Moe's in a new way since Twitter updated it's API.

    If none of these things have happened, please investigate."""
        )


def create_rts_frame(tweets):
    """Calculate a dataframe of retweets to analyze."""

    print("Manipulting data functions ...")
    try:
        # Convert the tweets dictionary to a dataframe
        tweets_data = pd.DataFrame.from_dict(tweets, orient="index")

        # Remove duplicates, taking only the largest retweet value
        individual_rts = (
            tweets_data.groupby(["user_id_str", "id_str"])["retweet_count"]
            .max()
            .reset_index()
        )

        return individual_rts

    # Raise this error if something weird happens
    except:
        raise SystemError(
            "\n\n\nUnexpected error occured creating dataframe of retweets."
        )


def create_user_rt_dict(individual_rts):
    """Create a Retweet Dictionary for All Users."""

    try:
        """
        Create a dictionary where the keys are user_ID numbers and
        the values are nested lists of all retweeet_count values for
        each individual user.

        For example:
        {
        user_id_num1 : [rt_count, rt_count, rt_count, ...],
        user_id_num2 : [rt_count, rt_count, rt_count, ...],
        user_id_num3 : [rt_count, ...],
        ...
         }

        """
        print("Counting retweets per user ...")

        user_rt_count_dict = {}

        for ii in range(len(individual_rts)):
            # Get the user at row ii
            user = individual_rts.loc[ii, "user_id_str"]
            # Get retweet_count values at ii
            rt_count = [individual_rts.loc[ii, "retweet_count"]]

            # If the user ID number is already present, add it to the existing list of RT values
            try:
                user_rt_count_dict[user].extend(rt_count)

            # If the user ID isn't present, an error will be raised and then we create the new `key : list` pair
            except:
                user_rt_count_dict.update({user: rt_count})

        return user_rt_count_dict

    # Raise this error if something weird happens...
    except Exception as e:
        raise Exception(
            f"\n\n\nUnexpected error occured while creating user retweet dictionary.\n{e}"
        )


def calculate_all_fib_scores(user_rt_count_dict):
    """Calculate All Users FIB Scores."""

    try:
        print("Calculating the FIB-index for all users ...")

        user_fib_index_dict = {}

        for user, rt_count_list in user_rt_count_dict.items():
            # Calculate the FIB-index from the list of retweet_count values
            fib_index = get_fib_scores(rt_count_list)

            # Add user and FIB-index values
            user_fib_index_dict.update({user: fib_index})

        # Convert the dictionary to a dataframe
        fib_index_frame = pd.DataFrame(
            {
                "user_id": list(user_fib_index_dict.keys()),
                "fib_index": list(user_fib_index_dict.values()),
            }
        )

        return fib_index_frame

    # Raise this error if something weird happens...
    except Exception as e:
        raise Exception(f"\n\n\nUnexpected error occured calculating FIB scores.\n{e}")


def select_users(fib_index_frame, threshold):
    """Select Users Based on Threshold"""

    print("Calculating the FIB-index value representative of the threshold ...")

    # Calculate the value at the percentile cutoff
    percentile_val = np.percentile(
        fib_index_frame.fib_index, threshold, interpolation="midpoint"
    )
    print(f"{threshold} Percentile = {percentile_val}")

    # Select only the users with an FIB-index above or equal to the threshold value
    print("Selecting users based on that threshold ...")
    fib_locs = fib_index_frame["fib_index"] >= percentile_val
    top_fibers = fib_index_frame.loc[fib_locs]
    top_fibers = top_fibers.sort_values(by="fib_index", ascending=False)

    return top_fibers


def write_file(top_fibers, threshold):
    """Write Data to Disk."""

    # Get todays date
    today = dt.datetime.strftime(dt.datetime.now(), "--%Y_%m_%d")

    # Create zipper of data
    zipper = zip(top_fibers.user_id, top_fibers.fib_index)

    # Set filename
    file_name = f"top-fibers{today}--thresh-{threshold}.csv"

    print(f"Writing users and their FIB-indices to the file : {file_name}")

    # Write file
    with open(file_name, "w+") as f:
        for uid, fib in zipper:
            f.write(f"{uid},{fib}\n")

    print(f"Users Selected  = {len(top_fibers)}")
    print(f"\n\nFin.\n")


# Execute the program
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == "__main__":
    data_files, threshold = parse_cl_args()
    tweets = load_tweets(data_files)
    individual_rts = create_rts_frame(tweets)
    user_rt_count_dict = create_user_rt_dict(individual_rts)
    fib_index_frame = calculate_all_fib_scores(user_rt_count_dict)
    top_fibers = select_users(fib_index_frame, threshold)
    write_file(top_fibers, threshold)
    print("Script Complete.")
