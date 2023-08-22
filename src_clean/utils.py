"""
PURPOSE:
    - This package contains a number of convenience functions
        which are used in scripts throughout this project's data
        analysis.

IMPORTANT:
    - If you are attempting to load this package outside of the `/src/`
        directory, this package will not load. This is because the
        `/src/` directory has not been added to the system PATH,
        which tells Python where to look for loading packages. To solve
        this problem, include the below lines of code.

        ~~~
        import sys

        # This adds the /src/ directory to the system path
        # so that we can import the utils.py package
        sys.path.insert(0, 'the/relative/path/to/src')

        import utils
        ~~~
"""
import argparse
import configparser
import datetime
import logging
import os

import numpy as np
import pandas as pd


def err_msg_w_path(path):
    return (
        f"Loader cannot find the file: <{path}>\n\n"
        "Note: Make sure your current directory is the "
        "same as where the script/notebook is being run."
    )


class Loader:
    """
    A function for loading data from the project.
    """

    def __init__(self):
        # User data from jan/feb decahose data
        # self._user_data_decahose = "../data/clean_account_data_decahose.pickle"
        # if not os.path.exists(self._user_data_decahose):
        #     raise Exception(err_msg_w_path(self._user_data_decahose))

        # User data from V2
        self._user_data = "../data/clean_account_data.csv"
        if not os.path.exists(self._user_data):
            raise Exception(err_msg_w_path(self._user_data))

        # User errors from V2
        self._user_errors = "../data/clean_errors_data.csv"
        if not os.path.exists(self._user_errors):
            raise Exception(err_msg_w_path(self._user_errors))

        # Top 181 influentials and fibers
        self._iffyp_top_inf_fib_path = (
            "../data/top_influentials_and_fibers_combined.txt"
        )
        if not os.path.exists(self._iffyp_top_inf_fib_path):
            raise Exception(err_msg_w_path(self._iffyp_top_inf_fib_path))

        # Iffy+ Jan through October tweet data (from Decahose)
        # self._iffyp_jan_oct_path = "../data/iffyp_jan-oct_all_clean--2021-07-07.pickle"
        # if not os.path.exists(self._iffyp_jan_oct_path):
        #     raise Exception(err_msg_w_path(self._iffyp_jan_oct_path))

        # Clean tweets for comparing FIBers to influentials
        self._iffyp_fib_vs_inf_clean_tweets_path = (
            "../data/iffyp_fib_inf_clean_tweets.parquet"
        )
        if not os.path.exists(self._iffyp_fib_vs_inf_clean_tweets_path):
            raise Exception(err_msg_w_path(self._iffyp_fib_vs_inf_clean_tweets_path))

        # Popular Baseline
        self._iffyp_pop_baseline_path = "../data/iffyp_popular_baseline.csv"
        if not os.path.exists(self._iffyp_pop_baseline_path):
            raise Exception(err_msg_w_path(self._iffyp_pop_baseline_path))

        # Influential Baseline
        self._iffyp_influential_baseline_path = "../data/iffyp_influential_baseline.csv"
        if not os.path.exists(self._iffyp_influential_baseline_path):
            raise Exception(err_msg_w_path(self._iffyp_influential_baseline_path))

        # Iffy+ domains files
        self._iffyp_domains_path = "../data/iffy_plus_full_original.csv"
        if not os.path.exists(self._iffyp_domains_path):
            raise Exception(err_msg_w_path(self._iffyp_domains_path))

        # Iffy+ Jan/Feb superspreaders all users
        self._iffyp_ss_all_path = "../data/top-fibers--2021_04_23--thresh-0.csv"
        if not os.path.exists(self._iffyp_ss_all_path):
            raise Exception(err_msg_w_path(self._iffyp_ss_all_path))

        # Iffy+ Jan/Feb superspreaders top users
        self._iffyp_ss_path = "../data/top-fibers--2021_02_19--thresh-99.csv"
        if not os.path.exists(self._iffyp_ss_path):
            raise Exception(err_msg_w_path(self._iffyp_ss_path))

        # Bot score for all tweets
        self._iffyp_bot_scores = "../data/iffyp_botometerLite_scores_raw.pickle"
        if not os.path.exists(self._iffyp_bot_scores):
            raise Exception(err_msg_w_path(self._iffyp_bot_scores))

        ###############################################################################################
        ###############################################################################################
        ###############################################################################################
        ###############################################################################################
        ###############################################################################################

    #######################################
    ##~~~~~~~## END OF __init__ ##~~~~~~~##
    #######################################

    def load_iffyp_top_infl_fiber_users(self):
        """
        Load the top 181 fibers and influential
        users. No argument inputs needed.

        Return
            - list of strings
        """
        with open(self._iffyp_top_inf_fib_path, "r") as f:
            list_of_users = [line.rstrip("\n") for line in f]

        return list_of_users

    def load_iffyp_baseline(self, name=str, as_dict=False):
        """
        This function loads the Iffy+ baseline dataframe of your
        choosing based on the `name` parameter.

        - When `name` = "popular", return pd.DataFrame
            with the columns: 'original_tweeter_user_id' (str) and
            'original_tweeter_f_count' (int) (aka num. of followers)

        - When `name` = "influential", return pd.DataFrame
            with the columns: 'original_tweeter_user_id' (str) and
            'retweet_count' (int)

        Parameters:
            - name ("popular" or "influential") : what data to return
            - as_dict (boolean) : when True, return a dictionary where
                the keys are user_ids (str) and values are the value
                of the respective dataframe

        """
        # If neither of these options are passed, throw an error
        if name not in ["popular", "influential"]:
            raise Exception(
                "`name` parameter must be either 'popular' or 'influential'"
            )

        # Load data based on `name` given
        if name == "popular":
            data = pd.read_csv(
                self._iffyp_pop_baseline_path, dtype={"original_tweeter_user_id": "str"}
            )
            # If as_dict = True, convert to a dictionary
            if as_dict:
                zipper = zip(
                    data["original_tweeter_user_id"], data["original_tweeter_f_count"]
                )
                data_dict = {u_id: f_count for u_id, f_count in zipper}
                return data_dict
            else:
                return data

        elif name == "influential":
            data = pd.read_csv(
                self._iffyp_influential_baseline_path,
                dtype={"original_tweeter_user_id": "str", "retweet_count": int},
            )
            if as_dict:
                zipper = zip(data["original_tweeter_user_id"], data["retweet_count"])
                data_dict = {u_id: rt_count for u_id, rt_count in zipper}
                return data_dict
            else:
                return data

    def load_iffyp_tweets_jan_2_oct(self):
        """
        This function takes no input and loads the
        Iffy+ tweets which includes data from January to October.

        Returns a pandas.DataFrame() object
        """

        raise Exception(
            "We cannot share this data. Please rehydrate and update the function."
        )

        return pd.read_pickle(self._iffyp_jan_oct_path)

    def load_iffyp_fib_vs_inf_clean_tweets(self):
        """
        This function takes no input and loads the
        tweet data for comparing the domain sharing behavior
        of the top FIBers to the top influential users.

        This data represents all tweets sent by the combined
        list of 181 top (FIBers and influential)
        users between Jan. and Mar. 2020.

        Returns a pandas.DataFrame() object
        """

        return pd.read_parquet(
            self._iffyp_fib_vs_inf_clean_tweets_path,
        )

    def load_iffyp_low_cred_domains(self):
        """
        Load a list of the Iffy+ low credibility domains
        """

        data = pd.read_csv(self._iffyp_domains_path, usecols=["Domain"])
        return list(data["Domain"])

    def load_user_data(self, name="decahose", info_type="data"):
        """
        Load users data gathered in different ways.

        Parameters:
        ----------
        name (str) : Which data file you'd like to load. Takes either
            `v2` or `decahose`.

            Options info:
            - decahose [default] = loads the user info we collected from the
                jan/feb decahose data
            - v2 = loads the user info recently downloaded with Twitter's
                V2 API

        info_type (str) : Type of information to load from the twitter v2
            data. Takes either `data` or `errors`. Note: only used when `v2`
            is passed for `name`

            Options info:
            - data [default] = load the user info we have for accounts not
                suspended
            - errors = loads the user IDs and errors that were returned
                by Twitter when we tried to query their user info

        Returns:
            pandas dataframe
        Exceptions:
            TypeError
        """

        if name == "decahose":
            raise Exception(
                "We cannot share this data. Please rehydrate and update the function."
            )
            return pd.read_pickle(self._user_data_decahose)

        # if `v2`, we also need to parse, which info_type we want
        elif name == "v2":
            if info_type == "data":
                return pd.read_csv(self._user_data, dtype={"u_id": str})

            elif info_type == "errors":
                return pd.read_csv(self._user_errors, dtype={"u_id": str})

            else:
                raise TypeError("`info_type` must be either 'data' or 'errors'")

        else:
            raise TypeError("`name` must be either 'decahose' or 'v2'")

    def load_iffyp_ss(self, all_users=False):
        """
        Loads the Iffy+ Jan/Feb superspreaders as
        identified by the FIB algorithm. Choose to return
        top 1% or all users with the `all_users` parameter.

        Parameters:
        - all_users (bool) : return FIB scores for ALL users?
            if True: Return all user ids and the FIB scores
            if False: (default) return only the top 1%

        Returns a pandas.DataFrame object
        """

        if all_users:
            data = pd.read_csv(
                self._iffyp_ss_all_path,
                names=["user_id", "fib_score"],
                dtype={"user_id": str, "fib_score": int},
            )
            return data
        else:
            data = pd.read_csv(
                self._iffyp_ss_path,
                names=["user_id", "fib_score"],
                dtype={"user_id": str, "fib_score": int},
            )
            return data

    def load_iffyp_tweet_bot_scores(self):
        """
        Load a pandas data frame of Botometer Lite
            scores for all tweets from the January/February
            Decahose data. Columns are:
            - tid (str)                          : tweet id number
            - user_id (str)                      : user id number
            - probe_timestamp (pandas timestamp) : time of tweet
            - bot_score_lite (numpy.float64)     : botometer lite score
        Note: Because we only consider tweets and retweets,
            there will be additional users in this file found from
            quote tweets, etc. - you will likely need to filter
            out these extra users for analysis.
        """

        return pd.read_pickle(
            self._iffyp_bot_scores,
        )


###########################################
##~~~~~~~## End of Loader class ##~~~~~~~##
###########################################


def parse_cl_args():
    """Set CLI Arguments."""
    print("Attempting to parse command line arguments...")

    try:
        # Initialize parser
        parser = argparse.ArgumentParser()
        # Add optional arguments
        parser.add_argument(
            "-c",
            "--config-file",
            metavar="Config-file",
            help="Full path to the project's config.ini file containing paths/file names for script.",
            required=True,
        )

        # Read parsed arguments from the command line into "args"
        args = parser.parse_args()
        print("Success.")
        # Assign the config file name to a variable and return it
        return args

    except Exception as e:
        print("Problem parsing command line input.")
        print(e)


def parse_config_file(config_file_path):
    """Parse config file from provided path"""

    try:
        config = configparser.ConfigParser()
        config.read(config_file_path)
        return config

    except Exception as e:
        print("Problem parsing config file.")
        print(e)


def convert_twitter_strings_2_dates(date):
    """
    A function for converting twitter date strings into
    datetime objects. It also handles NaN values smoothly
    by returning a NaT value.

    Note: All dates are converted to UTC prior to removing
    the timezone data object for simpler handling.
    """

    try:
        dtobj = datetime.datetime.strptime(date, "%a %b %d %H:%M:%S %z %Y")
        utc_dtobj = datetime.datetime.utcfromtimestamp(dtobj.timestamp())
        return utc_dtobj

    except TypeError as e:
        if np.isnan(date):
            return date
        else:
            raise TypeError(f"input type ({type(date)}) needs to be str.\n{e}")
    except Exception as e:
        raise Exception(f"Unknown error.\n\n{e}")
