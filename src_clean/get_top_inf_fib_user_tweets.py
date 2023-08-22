"""
PURPOSE:
    A script for utilizing the V2 full search endpoint (Academic track only)
    to pull tweets sent by a list of the top FIB-identified and "influential"
    users (those with the most retweets). The query is made with the `from:` operator.
        - Operators Reference: 
        - https://developer.twitter.com/en/docs/twitter-api/tweets/search/integrate/build-a-query
    
    Note: This script utilizes the osometweet package to query the Twitter API.
    - Ref: https://github.com/osome-iu/osometweet

INPUT:
    Project `config.ini` file with the following details included:
    ~~~~
    [VARS]
    GET_TWEETS_START_DATE = 2020-01-01
    GET_TWEETS_END_DATE = 2020-04-01

    [PATHS]
    TWTS_FROM_TOP_INFS_AND_FIBERS = /data_volume/super-spreaders/intermediate_files/tweets_from_top_infs_and_fibers

    [FILES]
    TOP_INF_AND_FIBERS_TWEETS = topuser_tweets.json
    ~~~

OUTPUT:
    One JSON file per input user that contains one tweet object per line. Each tweet
    will be representative of a tweet sent by that user.
        - Each filename will have the following format:
            - topuser_{USER_ID_NUM}_tweets.json

Author: Matthew DeVerna
"""

### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Load Packages ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
import datetime
import json
import os
import pytz
import subprocess
import time

import osometweet
import osometweet.fields as o_fields

from utils import parse_cl_args, parse_config_file
from utils import Loader


### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Set Functions ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def convert_date_to_iso(date):
    """Convert input string date to Twitter-friendly iso format"""

    print(f"Converting input date - {date} - to Twitter-friendly isoformat...")

    try:
        # Convert string date (YYYY-MM-DD) to datetime w/ timezone
        date = datetime.datetime.strptime(date, "%Y-%m-%d").replace(
            tzinfo=datetime.timezone.utc
        )

        # Convert datetime to isoformat
        date = datetime.datetime.strftime(date, "%Y-%m-%dT%H:%M:%S") + "Z"

        return date

    except Exception as e:
        print(e)
        raise Exception("Problem converting date to iso format!")


def initialize_osometweet():
    """Initialize osometweet authorization"""
    print("Initializing OsomeTweet API...")
    try:
        # Load bearer token
        bearer_token = os.environ.get("TWITTER_BEARER_TOKEN")

        # Break script if bearer token is not set
        if bearer_token is None:
            raise Exception("TWITTER_BEARER_TOKEN is not set!!")

        # Create OAuth2 authorization
        oauth2 = osometweet.OAuth2(bearer_token=bearer_token, manage_rate_limits=True)

        # Initialize OsomeTweet api with OAuth2 authorization
        ot = osometweet.OsomeTweet(oauth2)

        print("Success")
        return ot

    except Exception as e:
        print(e)
        raise Exception("Problem initializing OsomeTweet.")


def initialize_data_objects():
    """Initialize tweet fields"""

    print("Initializing Twitter V2 tweet fields for query...")
    try:
        tweet_fields = o_fields.TweetFields()
        tweet_fields.fields = ["id", "text", "entities", "created_at", "author_id"]

        print("Success.")
        return tweet_fields
    except Exception as e:
        print(e)
        raise Exception("Tweet field initialization failed.")


def get_latest_tweet_date(output_dir, file_name, end_date):
    """
    Get Twitter's created_at date from last recorded tweet

    Parameters:
        - output_dir (str) : the output directory
        - file_name (str) : name of the file
        - end_date (str) : the initial end date, input by the
            config.ini file. This is only used to pass this
            as an output if the file has been pulled and is
            empty (meaning they are suspended or similar)

    Return:
        end_date (str) : the end_date to use in the query.
            This may be updated or it may be the original,
            depending on the file.
    """
    try:
        # The last line will be the last tweet collected
        print("\tAttempting to get new end_date...")
        bash_command = f"tail -1 {os.path.join(output_dir,file_name)}"
        process = subprocess.run(bash_command.split(), stdout=subprocess.PIPE)

        earliest_tweet = json.loads(process.stdout.decode("UTF-8"))
        created_at = earliest_tweet["created_at"]

        # Convert string to dt obj to convert back to isoformat... (may be unnecessary)
        end_date = datetime.datetime.strptime(
            created_at, "%Y-%m-%dT%H:%M:%S.000Z"
        ).replace(
            tzinfo=pytz.UTC
        )  # - dt.timedelta(minutes = 1)
        end_date = datetime.datetime.strftime(end_date, "%Y-%m-%dT%H:%M:%S") + "Z"
        print(f"\tNew date: {end_date}")

        return end_date

    # Suspended users will create empty files. If we restart this
    # script this except block will catch that quirk
    # and it return the original end_date to try again
    except json.decoder.JSONDecodeError as e:
        if os.stat(os.path.join(output_dir, file_name)).st_size == 0:
            print("\tUser file was empty...")
            return end_date
        else:
            print(e)
            raise Exception("JSONDecodeError!! problem parsing latest tweet date")

    except Exception as e:
        print(e)
        raise Exception("Problem getting latest tweet date")


def get_tweets_of_user(
    user,
    start_date,
    end_date,
    tweet_fields,
    output_dir,
    ot,
):
    """
    Pull tweet data for individual user.

    Parameters:
    -----------
    - user (str) : the user ID to pull tweets for
    - start_date (str) : the start date to pull tweets for
    - end_date (str) : the end date to pull tweets for
    - tweet_fields (osometweet object) : the tweet fields to pull, created with osometweet
    - output_dir (str) : the output directory where tweets will be saved
    - ot (osometweet object) : the initialized OsomeTweet object for api calls

    Returns:
    -----------
    None --- the functions writes tweets to a file for each user.
    """
    print(f"Pulling user: {user}")

    tweet_count = 0

    file_name = f"topuser_{user}_tweets.json"

    # The function is designed to handle broken pulls. To do this, we check if the
    # file already exists and, if it does, update the end_date. We update the end date
    # because Twitter pulls in reverse chronological order.
    if os.path.exists(os.path.join(output_dir, file_name)):
        end_date = get_latest_tweet_date(
            output_dir=output_dir, file_name=file_name, end_date=end_date
        )

    # Exit if the new end_date is the start date
    end_date_dt_obj = datetime.datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%SZ")
    if end_date_dt_obj.date() == datetime.datetime(2020, 1, 1).date():
        print(f"Already pulled all data for user: {user}")
        return

    try:
        # Open file for a user and get initial query
        with open(os.path.join(output_dir, file_name), "a+") as user_file:
            time.sleep(1)
            response = ot.search(
                query=f"from:{user}",
                fields=tweet_fields,
                start_time=start_date,
                end_time=end_date,
                max_results=100,
                full_archive_search=True,
            )

            try:
                # Sometimes Twitter returns no data but does include
                # a next_token. So we only write data if it is present.
                # Otherwise, we move to the next try statement
                if response.get("data") is None:
                    pass
                else:
                    tweets = response["data"]
                    for tweet in tweets:
                        tweet_count += 1
                        user_file.write(f"{json.dumps(tweet)}\n")

                try:
                    empty_requests = 0

                    # If the next_token is present, we need to keep querying
                    # If it's not there, then we've already got all the data
                    while "next_token" in response["meta"]:
                        time.sleep(1)
                        response = ot.search(
                            query=f"from:{user}",
                            fields=tweet_fields,
                            start_time=start_date,
                            end_time=end_date,
                            max_results=100,
                            next_token=response["meta"]["next_token"],
                            full_archive_search=True,
                        )

                        # Write subsequent queries to data file
                        if response.get("data") is None:
                            empty_requests += 1

                            # If we get 25 empty requests IN A ROW
                            # which have a "next_token" we want to
                            # break the while-loop
                            if empty_requests == 25:
                                break

                            else:
                                pass

                        else:
                            empty_requests = 0
                            tweets = response["data"]
                            for tweet in tweets:
                                tweet_count += 1
                                user_file.write(f"{json.dumps(tweet)}\n")

                    print(f"Success.")
                    print(f"\tTotal tweets pulled = {tweet_count}")

                except Exception as e:
                    print(e)
                    raise Exception(
                        f"Problem encountered during the pagination process for user: {user}"
                    )

            except Exception as e:
                print(e)
                raise Exception(
                    f"Problem with accessing the response['data'] object for user: {user}"
                )

    except Exception as e:
        print(e)
        raise Exception(f"Problem with initial query for user: {user}")


### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Execute Script ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == "__main__":
    # Load the config file variables
    args = parse_cl_args()
    config = parse_config_file(args.config_file)

    # Initialize our Loader class and load the necessary (str) user IDs
    l = Loader()
    top_infl_fiber_users = l.load_iffyp_top_infl_fiber_users()

    # Check which files exist already and remove not needed users
    output_dir = config["PATHS"]["TWTS_FROM_TOP_INFS_AND_FIBERS"]

    # Convert start and end dates to proper iso format
    start_date = convert_date_to_iso(config["VARS"]["GET_TWEETS_START_DATE"])
    end_date = convert_date_to_iso(config["VARS"]["GET_TWEETS_END_DATE"])

    # Initialize osometweet and get data objects/fields
    ot = initialize_osometweet()
    tweet_fields = initialize_data_objects()

    for user in top_infl_fiber_users:
        try:
            get_tweets_of_user(
                user,
                start_date,
                end_date,
                tweet_fields,
                output_dir,
                ot,
            )

        # Record problem users
        except Exception as e:
            print(f"~~!! Problematic user: {user}", e)
            pass

    print("~~~ Script Complete ~~~")
