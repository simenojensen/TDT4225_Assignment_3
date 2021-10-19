# -*- coding: utf-8 -*-
"""Code to create and fill `TDT4225ProjectGroup78` MongoDB database with data.

This module contains code that creates the `TDT4225ProjectGroup78` MongoDB
database, creates the collections of the `TDT4225ProjectGroup78` database, and
fills the database with data. The module also provides an interface to queries
performed on the database.

"""
import os
import pandas as pd
import numpy as np
from pymongo import MongoClient
import time

import queries
import importlib
importlib.reload(queries)


def parse_data():
    """Parse data from `.plt` files into collection dictionaries.

    Returns
    -------
    user_dict : dict
        Collection of users.
    activity_dict : dict
        Collection of users.
    trackpoint_dict : dict
        Collection of trackpoints.
    """
    # Load user data into Pandas DataFrame
    user_ids = sorted(os.listdir("../dataset/Data/"))
    # remove Mac Finder generated files
    user_ids = [uid for uid in user_ids if not uid.startswith(".")]
    # Find labeled users
    with open("../dataset/labeled_ids.txt", "r") as f:
        labeled_users = f.read().splitlines()
    has_labels = [True if uid in labeled_users else False for uid in user_ids]
    user_df = pd.DataFrame({"id": user_ids, "has_labels": has_labels})

    # Trackpoint columns
    trackpoint_cols = ["lat", "lon", "ignore", "altitude", "date_days", "date", "time"]

    # lists to store dataframes
    trackpoint_ll = []
    activity_ll = []

    aid = 0
    for uid in user_ids:
        user_path = f"../dataset/Data/{uid}/"
        trajectory_path = user_path + "Trajectory/"

        # Load labels if they exist
        labels = []
        if os.path.exists(user_path + "labels.txt"):
            labels = pd.read_csv(user_path + "labels.txt", sep="\t")
            labels["Start Time"] = pd.to_datetime(labels["Start Time"])
            labels["End Time"] = pd.to_datetime(labels["End Time"])

        for filename in os.listdir(trajectory_path):
            # Load trackpoints
            df = pd.read_csv(
                trajectory_path + filename, skiprows=6, names=trackpoint_cols
            )
            # Ignore if more than 2500 records
            if len(df) > 2500:
                continue
            # Convert to datetime
            df["date_time"] = pd.to_datetime(df["date"] + " " + df["time"])
            df = df.drop(columns=["date", "time", "ignore"])

            # Create activity record
            activity = {}
            activity["id"] = aid
            activity["user_id"] = uid
            activity["start_date_time"] = df["date_time"].iloc[0]
            activity["end_date_time"] = df["date_time"].iloc[-1]
            activity["transportation_mode"] = np.nan

            # Find transportation mode
            # Makes sure that duplicate labels are handled by adding additional
            # Activities
            if len(labels) > 0:
                # Find labels that matches the current trackpoint start and
                # end time
                temp_df = labels.loc[
                    (labels["Start Time"] == activity["start_date_time"])
                    & (labels["End Time"] == activity["end_date_time"])
                ]
                # If empty, add current activity to list
                if len(temp_df) == 0:
                    # Add aid to trackpoint
                    df["activity_id"] = aid

                    trackpoint_ll.append(df)
                    activity_ll.append(pd.Series(activity))

                    # increment aid
                    aid += 1
                # Else, loop through entries in the labels and add new
                # activities for each match
                else:
                    for tm in temp_df["Transportation Mode"].values:
                        # Add aid to trackpoint
                        df["activity_id"] = aid

                        # Create new activity
                        activity = {}
                        activity["id"] = aid
                        activity["user_id"] = uid
                        activity["start_date_time"] = df["date_time"].iloc[0]
                        activity["end_date_time"] = df["date_time"].iloc[-1]
                        activity["transportation_mode"] = tm

                        trackpoint_ll.append(df)
                        activity_ll.append(pd.Series(activity))

                        # increment aid
                        aid += 1
            # If there's no match, add current activity
            else:
                # Add aid to trackpoint
                df["activity_id"] = aid
                trackpoint_ll.append(df)

                activity_ll.append(pd.Series(activity))
                # increment aid
                aid += 1

    # Create dataframes from saved lists
    trackpoint_df = pd.concat(trackpoint_ll).reset_index(drop=True)
    trackpoint_df["id"] = [i for i in range(len(trackpoint_df))]
    activity_df = pd.DataFrame().append(activity_ll)

    # Replace -777 as it is an invalid altitude
    trackpoint_df["altitude"].replace(-777, np.nan, inplace=True)

    # Changes to data structures for mongoDB
    # Rename columns
    user_df = user_df.rename(columns={"id": "_id"})
    activity_df = activity_df.rename(columns={"id": "_id"})
    trackpoint_df = trackpoint_df.rename(columns={"id": "_id"})

    # Create list of activity ids in user_df cells
    # use frozenset as it is hashable, then convert back to list
    temp_df = activity_df.groupby(["user_id"]).agg(
        activity_id=pd.NamedAgg(column="_id", aggfunc=frozenset)
    )
    user_df = pd.merge(
        left=user_df, right=temp_df, left_on="_id", right_on=temp_df.index, how="left"
    )
    user_df["activity_id"] = user_df["activity_id"].apply(
        lambda x: list(x) if not pd.isnull(x) else []
    )
    # Drop column
    activity_df = activity_df.drop(columns=["user_id"])

    # Create dicts
    user_dict = user_df.to_dict("records")
    activity_dict = activity_df.to_dict("records")
    trackpoint_dict = trackpoint_df.to_dict("records")

    return (user_dict, activity_dict, trackpoint_dict)


def create_user(USER, PASSWORD, HOST, DB_NAME):
    """Create MongoDB user for the `TDT4225ProjectGroup78` database.

    First drops the database, then creates a user with the given login
    information.

    Parameters
    ----------
    USER : str
        The entered MongoDB user.
    PASSWORD : str
        The entered MongoDB password.
    HOST : str
        The entered MongoDB host.
    DB_NAME : str
        The MongoDB database name (`TDT4225ProjectGroup78`).

    """
    uri = f"mongodb://{USER}:{PASSWORD}@{HOST}/{DB_NAME}"
    with MongoClient(uri) as client:
        # Start by dropping the database.
        client.drop_database(DB_NAME)

        # Add the user to the database.
        db = client[DB_NAME]
        db.add_user(USER,PASSWORD)


def insert_data(USER, PASSWORD, HOST, DB_NAME):
    """Create collections and insert data.

    Inserts the parsed data from the `.plt` files into the
    `TDT4225ProjectGroup78` database.

    Parameters
    ----------
    USER : str
        The entered MongoDB user.
    PASSWORD : str
        The entered MongoDB password.
    HOST : str
        The entered MongoDB host.
    DB_NAME : str
        The MongoDB database name (`TDT4225ProjectGroup78`).

    """
    start_time = time.time()
    user_dict, activity_dict, trackpoint_dict = parse_data()
    print(
        f"Data parsed successfully. Time taken: {time.time() - start_time:.2f} seconds"
    )

    uri = f"mongodb://{USER}:{PASSWORD}@{HOST}/{DB_NAME}"
    with MongoClient(uri) as client:
        # Create database
        db = client[DB_NAME]

        # Create collections
        user = db["user"]
        activity = db["activity"]
        trackpoint = db["trackpoint"]

        # Insert data
        start_time = time.time()
        user.insert_many(user_dict)
        print(
            f"User collection created successfully. Time taken: {time.time() - start_time:.2f} seconds"
        )

        start_time = time.time()
        activity.insert_many(activity_dict)
        print(
            f"Activity collection created successfully. Time taken: {time.time() - start_time:.2f} seconds"
        )

        start_time = time.time()
        trackpoint.insert_many(trackpoint_dict)
        print(
            f"Trackpoint collection created successfully. Time taken: {time.time() - start_time:.2f} seconds"
        )


def query_database(USER, PASSWORD, HOST, DB_NAME):
    """Call the different query functions.

    Parameters
    ----------
    USER : str
        The entered MongoDB user
    PASSWORD : str
        The entered MongoDB password
    HOST: str
        The entered MongoDB host
    DB_NAME : str
        The MongoDB database name (`TDT4225ProjectGroup78`)
    """

    # Instantiate connection
    uri = f"mongodb://{USER}:{PASSWORD}@{HOST}/{DB_NAME}"
    with MongoClient(uri) as client:

        db = client["TDT4225ProjectGroup78"]
        user = db["user"]
        activity = db["activity"]
        trackpoint = db["trackpoint"]

        # Query 1
        print("Query 1:")
        queries.query_1(user, activity, trackpoint)

        # # Query 2
        # print("Query 2:")
        # queries.query_2(user)

        # # Query 3
        # print("Query 3:")
        # queries.query_3(user)

        # # Query 4
        # print("Query 4:")
        # queries.query_4(activity)

        # # Query 5
        # print("Query 5:")
        # queries.query_5(activity)

        # # Query 6
        # print("Query 6:")
        # queries.query_6(user, trackpoint)

        # # Query 7
        # print("Query 7:")
        # queries.query_7(user, activity)

        # # Query 8
        # print("Query 8:")
        # queries.query_8(activity)

        # # Query 9
        # print("Query 9:")
        # queries.query_9(user, activity)

        # # Query 10
        # print("Query 10:")
        # queries.query_10(user, activity, trackpoint)

        # Query 11
        # print("Query 11:")
        # queries.query_11(trackpoint)

        # Query 12
        # print("Query 12:")
        # queries.query_12(trackpoint)
