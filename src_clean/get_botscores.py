"""
PURPOSE:
    A script to estimate the botscores for all tweets in the Jan/Feb data
        with the Botometer Lite models.

    REPLICATION NOTES:
    - We are using the raw Botometer Lite models which can be accessed
        by the public via the BotometerLite API
        - BotometerLite Paper: 
            https://ojs.aaai.org//index.php/AAAI/article/view/5460
        - API References:
            https://botometer.osome.iu.edu/
            https://github.com/IUNetSci/botometer-python
            https://rapidapi.com/OSoMe/api/botometer-pro/

INPUT:
    Project `config.ini` file with the following details included:
    ~~~~
    [VARS]

    [PATHS]
    BOTS_DATA_DIR = /data_volume/super-spreaders/intermediate_files/bot_detection/data
    IFFYP_MOES_FOLDER = /data_volume/super-spreaders/moes_data/jan-oct_iffy_plus

    [FILES]
    IFFYP_BOTOMETER_LITE_SCORES = iffyp_botometerLite_scores_raw.pickle
    ~~~

OUTPUT:
    - A .pickle file of botscores
    - See get_bot_scores() function below for more detail

Author: Matthew DeVerna
"""
### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Load packages ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
import json
import os
import sys

import pandas as pd

sys.path.insert(
    0,
    "/data_volume/super-spreaders/intermediate_files/bot_detection/BotometerLite-modelselection",
)
from BotometerLite.core import BotometerLiteDetector
from utils import parse_config_file, parse_cl_args

### ~~~~~~~~~~~~~~~ Set a couple absolute paths for botometer lite ~~~~~~~~~~~~~~~
B_LITE_MODEL_PATH = "/data_volume/super-spreaders/intermediate_files/bot_detection/BotometerLite-modelselection/models/BEV-best_skl-0.22.2.pkl.gz"
BIGRAM_PATH = "/data_volume/super-spreaders/intermediate_files/bot_detection/BotometerLite-modelselection/models/screen_name_bigram_freq.pkl.gz"


### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Create functions ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def find_jan_feb_tweet_paths(moes_path):
    """
    Retrieve the full paths for the raw January and February tweets.

    Parameters:
        - moes_path (str) : full paths to moes data directory

    Returns:
        - tweet_files (list) : list of tweet file paths
    """

    print("Trying to get tweet file paths...")
    try:
        # From the larger moes data path, grab only the jan/feb tweet files
        tweet_files = []
        for root, dirs, files in os.walk(moes_path):
            # Make sure we're in the jan/feb tweetContent directory
            if "01_Jan-Feb" in root and root.endswith("tweetContent"):
                # Append full path of only the tweetContent files
                for file in files:
                    if "part-m-" in file:
                        tweet_files.append(os.path.join(root, file))
        try:
            assert len(tweet_files) == 2
        except:
            raise (
                "More/less than two tweet files found!",
                f"\n\nWhat was found:{tweet_files}",
            )
        print("Success.")
        return tweet_files

    except:
        raise Exception("Problem loading jan/feb tweet data!")


def load_tweets(tweet_files):
    """
    Load the tweets from the raw January and February tweets.

    Parameters:
        - tweet_files (list) : list of full paths to tweet files

    Returns:
        - tweets (list) : list of tweet obejcts
    """
    print("Trying to load tweets...")
    try:
        # Load tweets
        tweets = []
        for file in tweet_files:
            with open(file, "r") as f:
                for line in f:
                    tweet = json.loads(line)
                    tweets.append(tweet)
        print("Success.")
        return tweets

    except:
        raise Exception("Problem loading jan/feb tweet data!")


def get_bot_scores(tweets, model_path, bigram_path):
    """
    Use botometer lite to get botscores from the tweet object.

    Parameters:
    -----------
    - tweets - a list of V1 tweet obejcts
    - model_path - the full path to the BL model
    - bigram_path - full path to the bigram model

    Returns:
    -----------
    - results : pandas dataframe where columns are:
        'tid' - tweet id str
        'user_id' - user id str
        'probe_timestamp' - timestamp of tweet
        'bot_score_lite' - botometer lite score
    """
    print(f"Working to pull botometerLite scores for {len(tweets)} tweets...")
    try:
        bot_classifier = BotometerLiteDetector(model_path, bigram_path)
        results = bot_classifier.detect_on_tweet_objects(tweet_objects=tweets)
        return results
    except:
        raise Exception("Problem calculating botscores with botometerlite!")


### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Execute script ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == "__main__":
    # Load the config file variables
    args = parse_cl_args()
    config = parse_config_file(args.config_file)

    # Load input/output paths/files
    try:
        moes_path = config["PATHS"]["IFFYP_MOES_FOLDER"]
        botscores_path = config["PATHS"]["BOTS_DATA_DIR"]
        raw_output_name = config["FILES"]["IFFYP_BOTOMETER_LITE_SCORES"]

    except:
        raise Exception("Problem loading files from config.ini")

    # Load tweet data
    tweet_files = find_jan_feb_tweet_paths(moes_path)
    tweets = load_tweets(tweet_files)

    # Calculate botometer scores
    results = get_bot_scores(
        tweets=tweets, model_path=B_LITE_MODEL_PATH, bigram_path=BIGRAM_PATH
    )
    results["tid"] = results["tid"].astype(str)
    results["user_id"] = results["user_id"].astype(str)

    # Save files
    try:
        raw_results_fname = os.path.join(botscores_path, raw_output_name)
        results.to_pickle(raw_results_fname)
    except:
        raise Exception("Problem saving output files...")

    print("~~~ Script Complete ~~~")
