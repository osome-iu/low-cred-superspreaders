"""
PURPOSE:
    A script to download the current status of users in the Jan/Feb
    Decahose data.

INPUT:
    Project `config.ini` file with the following details included:
    ~~~~
    [VARS]

    [PATHS]
    USER_DATA_DIR = /data_volume/super-spreaders/intermediate_files/user_data

    [FILES]
    ~~~

OUTPUT:
    - Two output files, one for the user data we were able to retrieve
        and another for the errors that were returned by Twitter for
        each user.

Author: Matthew DeVerna
"""
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
import json
import os
from datetime import datetime as dt

import osometweet
from osometweet.utils import chunker
from utils import parse_config_file, parse_cl_args, Loader


# Create Functions.
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def load_keys():
    """
    Load Twitter Keys from local environment.

    Note:
    I load and use the "user auth" tokens instead of the "app auth"
    token (bearer_token) because it allows us 3x more (900 vs. 300)
    requests per 15 minutes.
    """
    print("Trying to load environment variable Twitter tokens...")

    # If nothing is found, `os` returns `None`
    access_token = os.environ.get("TWITTER_ACCESS_TOKEN")
    access_token_secret = os.environ.get("TWITTER_ACCESS_TOKEN_SECRET")
    api_key = os.environ.get("TWITTER_API_KEY")
    api_key_secret = os.environ.get("TWITTER_API_KEY_SECRET")

    tokens = [
        ("access_token", access_token),
        ("access_token_secret", access_token_secret),
        ("api_key", api_key),
        ("api_key_secret", api_key_secret),
    ]

    # Print any tuples containing a `None` value
    # This just checks to see we forgot to set our environment variables.
    switch = False
    for token_tuple in tokens:
        if token_tuple[1] is None:
            print(token_tuple)
            switch = True
    # Raise exception if this happens
    if switch:
        raise Exception("Problem loading the Twitter tokens!!")

    print("Success.")
    return access_token, access_token_secret, api_key, api_key_secret


def gather_data(
    access_token,
    access_token_secret,
    api_key,
    api_key_secret,
    chunked_user_list,
    output_dir,
):
    print("Gathering Data...")

    # Set up authorization
    oauth1a = osometweet.OAuth1a(
        api_key=api_key,
        api_key_secret=api_key_secret,
        access_token=access_token,
        access_token_secret=access_token_secret,
    )
    ot = osometweet.OsomeTweet(oauth1a)

    # Add all user_fields
    all_user_fields = osometweet.UserFields(everything=True)

    # Get today's date
    today = dt.strftime(dt.today(), "%Y-%m-%d_%H-%M")

    # Create the output files...
    data_path = os.path.join(output_dir, f"account_data--{today}.json")
    errors_path = os.path.join(output_dir, f"account_errors--{today}.json")

    # Open two files. One for good data, the other for account errors.
    with open(data_path, "w") as data, open(errors_path, "w") as errors:
        # Iterate through the list of lists
        for num, one_hundred_users in enumerate(chunked_user_list, start=1):
            print(f"Working on chunk {num} of {len(chunked_user_list)} chunks")

            response = ot.user_lookup_ids(
                user_ids=one_hundred_users, fields=all_user_fields
            )

            # Get data and errors
            resp_data = response["data"]
            resp_errors = response["errors"]

            # No matter what `data` and `errors` will return something,
            # however, they may return `None` (i.e. no data/errors), which
            # will throw a TypeError.
            try:
                data.writelines(f"{json.dumps(line)}\n" for line in resp_data)
            except TypeError:
                print(
                    "No USER data found in this set of users"
                    ", skipping to the next set."
                )
                pass

            try:
                errors.writelines(f"{json.dumps(line)}\n" for line in resp_errors)
            except TypeError:
                print(
                    "No problematic users found in this set of user"
                    ", skipping to the next set."
                )
                pass


# Execute the program
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == "__main__":
    # Load the config file variables
    args = parse_cl_args()
    config = parse_config_file(args.config_file)
    output_dir = config["PATHS"]["USER_DATA_DIR"]

    # Initialize our Loader class and load the needed data
    print("Loading users...")
    try:
        l = Loader()
        # It doesn't matter what baseline we use, they contain the same users
        users_frame = l.load_iffyp_baseline(name="popular")
        users_list = list(set(users_frame["original_tweeter_user_id"]))

        # Convert our list of users into multiple lists of max 100 users
        max_query_length = 100
        chunked_user_list = chunker(seq=users_list, size=max_query_length)
    except Exception as e:
        print(e)
        raise Exception("Problem loading users!")
    print("Success.")

    access_token, access_token_secret, api_key, api_key_secret = load_keys()

    gather_data(
        access_token=access_token,
        access_token_secret=access_token_secret,
        api_key=api_key,
        api_key_secret=api_key_secret,
        chunked_user_list=chunked_user_list,
        output_dir=output_dir,
    )

    print("Data pull complete.")
