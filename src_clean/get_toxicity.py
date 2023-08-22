"""
PURPOSE:
    A script for downloading the probability of tweets being toxic maping each tweet ID
        to it's toxicity probability. This script utilizes the Perspective API to gather
        this data.
        - Ref: https://developers.perspectiveapi.com/s/about-the-api
    This file also stores the tweet IDs for language errors* that the API cannot handle
        and can be restarted if the script falls over midway through data collection,
        without re-querying the same tweet IDs again. It does this by creating a file
        that contains the tweet ID for tweets where language errors were encountered
        and reading previously recorded data files tweet IDs. The language error
        and previously recorded tweet IDs are then removed from the full list of tweet
        IDs to query.
    * Language errors are either:
        1. The API does not handle the language in the tweet (e.g. Japanese is not supported)
        2. The API cannot recognize the language in the tweet

INPUT:
    Project `config.ini` file with the following details included:
    ~~~~
    [VARS]

    [PATHS]
    TOXICITY_DIR = /data_volume/super-spreaders/intermediate_files/toxicity

    [FILES]
    IFFYP_TOXICITY_PROBS = iffyp_tweet_toxicity_probabilities.csv
    ~~~

OUTPUT:
    Files containing all toxicity probabilities for each tweet.
        - iffyp_tweet_toxicity_probabilities{int}.csv
            - Integers are included in the fname for restarts
        - rows contain one tweet per row
        - columns = ["tweet_id", "toxicity_score"]
    language_error_tweets.txt - file where each line contains a single tweet ID
        and each represents a previously queried tweet ID that caused the Perspective
        API to throw a language error.

Author: Matthew DeVerna
"""

### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Load Packages ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
import csv
import os
import glob
import httplib2
import json
import random
import time
import sys

import numpy as np
import pandas as pd

from googleapiclient import discovery
from utils import parse_config_file, parse_cl_args, Loader


### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Create Functions ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def switch_api_keys(api_key=str, all_keys=list):
    """
    Return the next api key from the provided list

    Parameters:
        - api_key (str) : the last used api_key
        - all_keys (list) : a list of all available
            api keys

    Returns:
        - the next api key in the string. If the
            last used key was the last key in the
            list, return the first key in the list.
    """
    print("Switching keys...")

    idx = all_keys.index(api_key) + 1

    # If the next api key is the last one,
    # Change the index to 0 to select the first key
    if idx == len(all_keys):
        idx = 0
    return all_keys[idx]


def get_toxicity(tweet_text=str, api_key=str, tries=int, all_keys=list):
    """
    Query the Perspective API for toxicity scores, implement exponentially
        larger and larger wait periods based on number of retries, and
        cycle through provided keys when being rate limited.
        - Ref: https://developers.perspectiveapi.com/s/about-the-api

    Parameters:
        - tweet_text (str) : full tweet text
        - api_key (str) : the API key to utilize for the current call
        - tries (int) : how many attempts have been made already. This
            controls how long the exponential back-off function will wait
        - all_keys (list) : all possible keys to use. This still works if
            only one key is provided, but it must be placed into a list.

    Returns:
        - 'AnalyzeComment' response (dict) : a response from the
            Perspective API including only a toxicity probability
    """

    client = discovery.build(
        "commentanalyzer",
        "v1alpha1",
        developerKey=api_key,
        discoveryServiceUrl="https://commentanalyzer.googleapis.com/$discovery/rest?version=v1alpha1",
    )

    analyze_request = {
        "comment": {"text": tweet_text},
        "requestedAttributes": {"TOXICITY": {}},
    }

    try:
        # If we get a successful request, set switch to False.
        # This will break the while loop in the main script and go
        # to the next tweet
        response = client.comments().analyze(body=analyze_request).execute()
        switch = False

    # A server error we hit when `commentanalyzer.googleapis.com`
    # is not available
    except httplib2.error.ServerNotFoundError as e:
        print(e)
        print("Can't find server, waiting 15 seconds...")
        time.sleep(15)

        response = None
        tries += 1
        switch = True

        # Switch to the next API key, just in case
        api_key = switch_api_keys(api_key=api_key, all_keys=all_keys)

    # A server error we hit when the network is unreachable
    except OSError as e:
        print(e)
        print("Handling OS ERROR, waiting 15 seconds...")
        time.sleep(15)

        response = None
        tries += 1
        switch = True

        # Switch to the next API key, just in case
        api_key = switch_api_keys(api_key=api_key, all_keys=all_keys)

    # For other errors, we check the status code
    except Exception as e:
        # 429 = "rate limiting"
        # Employ an exponential backoff procedure
        if e.status_code == 429:
            secs_2_wait = (2**tries) + (random.uniform(0, 1))

            # We shouldn't have to wait more than two minutes
            if secs_2_wait > 120:
                secs_2_wait = 120

            print(f"Waiting {secs_2_wait} seconds...")
            time.sleep(secs_2_wait)
            response = None
            tries += 1
            switch = True  # Ensures we try the same tweet again

            # Switch to the next API key, to minimize wait period
            api_key = switch_api_keys(api_key=api_key, all_keys=all_keys)

        # 400 = "bad request"
        # This catches queries that break because the tweet contains a
        # language not covered by the API
        elif (e.status_code == 400) and ("language" in e._get_reason()):
            response = "language_error"
            switch = False  # We can't fix this, so we move to the next tweet

        # Sometimes the language is undetectable, this handles that
        elif (e.status_code == 400) and ("language" in e.error_details):
            response = "language_error"
            switch = False  # We can't fix this, so we move to the next tweet

        else:
            # If none of the above, we are getting some weird error
            # wait 30 seconds and try again.
            print("UNKNOWN ERROR!! Waiting for 30 seconds...")
            print("~~STATUS CODE~~", e.status_code)
            print("~~ERROR DETAILS~~", e.error_details)
            print(e)
            response = None
            time.sleep(30)
            switch = False

            api_key = switch_api_keys(api_key=api_key, all_keys=all_keys)

    return response, switch, tries, api_key


### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Create Functions ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

if __name__ == "__main__":
    # Load API Keys
    comma_separated_api_keys = os.environ.get("PERSPECTIVE_API_KEY")
    if comma_separated_api_keys is None:
        raise Exception("YOU FORGOT TO SET YOUR API KEY AS AN ENVIRONMENT VARIABLE!!")

    # Separate keys into a list and take the first one
    # NOTE: We utilize multiple API keys at once to reduce wait times. The waiting
    #       strategy employed here may not work if you only utilize one key.
    list_of_api_keys = comma_separated_api_keys.split(",")
    print(f"You are working with {len(list_of_api_keys)} different keys.")
    api_key = list_of_api_keys[0]

    # Load the config file variables
    args = parse_cl_args()
    config = parse_config_file(args.config_file)

    # Initialize our Loader class and load the necessary data
    l = Loader()
    data = l.load_iffyp_tweets_jan_2_oct()

    # Retweets have the same text as the original tweet
    # Drop duplicates to reduce the number of tweets to query
    data = data.drop_duplicates("original_tweet_id").reset_index(drop=True)

    # Set output filename
    out_dir = config["PATHS"]["TOXICITY_DIR"]
    out_name = config["FILES"]["IFFYP_TOXICITY_PROBS"]

    # Get all data files in output directory
    existing_files = glob.glob(os.path.join(out_dir, "*.csv"))

    # If files already exists, append an integer to the end of the file name
    # to create a new output file name
    num_existing_files = len(existing_files)
    if num_existing_files > 0:
        out_name = f"{num_existing_files}.".join(out_name.split("."))

    output_file_name = os.path.join(out_dir, out_name)

    # Double check it doesn't exist...
    if os.path.exists(output_file_name):
        raise Exception(
            "ERROR: file name already exists. Breaking script to not over write already pulled data."
        )

    # Now we gather all tweet_ids that we've already pulled or for which
    # we encountered a language issue
    prev_checked_t_ids = []

    # Check if there is a language error file and add all tweet ids in there to the list
    language_error_fname = os.path.join(out_dir, "language_error_tweets.txt")
    if os.path.exists(language_error_fname):
        with open(language_error_fname, "r") as error_f:
            _ = [prev_checked_t_ids.append(t_id.strip("\n")) for t_id in error_f]

    # Check completed files and add those tweet ids as well
    if num_existing_files > 0:
        for file in existing_files:
            prev_pulled_data = pd.read_csv(file, dtype={"tweet_id": str})
            prev_tweet_ids = list(prev_pulled_data["tweet_id"])
            prev_checked_t_ids.extend(prev_tweet_ids)

    # Remove rows from our data frame with those tweet ids
    data = data[~data["original_tweet_id"].isin(prev_checked_t_ids)]

    # Create zipper to iterate over only what we need
    tweetId_tweetText_zipper = zip(data["original_tweet_id"], data["text"])

    # Some helpful numbers for printing loop progress
    number_of_queries = len(data)
    update_chunk_size = 5000  # Print updates after this many tweets
    total_chunks = np.ceil(number_of_queries / update_chunk_size)
    count = 0
    chunk = 0

    print(f"Pulling data on {number_of_queries} tweets...")

    # We open an output file to save data as we go because
    # the script will take a long time to complete
    with open(output_file_name, "w") as f_out:
        csv_out = csv.writer(f_out)
        csv_out.writerow(["tweet_id", "toxicity_score"])

        # Loop through each tweet ID and it's full text
        for t_id, text in tweetId_tweetText_zipper:
            # Print updates
            count += 1
            if count % update_chunk_size == 0:
                chunk += 1
                print(
                    f"5k tweets processed || {chunk} chunks completed out of {int(total_chunks)}"
                )

            switch = True
            tries = 1
            while switch:
                # Wait 50 milliseconds before each call
                time.sleep(0.05)

                # If this query is successful, `switch` is returned False.
                # Otherwise, it returns True and waits exponentially longer
                # after each try.
                try:
                    response, switch, tries, api_key = get_toxicity(
                        tweet_text=text,
                        api_key=api_key,
                        tries=tries,
                        all_keys=list_of_api_keys,
                    )
                except OSError as e:
                    print(e)
                    print("Handling OS ERROR, wait 10 seconds...")
                    time.sleep(10)

            # Handle errors by skipping to next tweet id
            if response is None:
                continue
            # If we get a language error, save that tweet ID
            elif response == "language_error":
                with open(language_error_fname, "a") as error_f:
                    error_f.write(f"{t_id}\n")
                continue
            # Save tweet id and score if request successful
            else:
                toxicity_score = response["attributeScores"]["TOXICITY"][
                    "summaryScore"
                ]["value"]
                csv_out.writerow((t_id, toxicity_score))

    print("~~~ Script Complete ~~~")
