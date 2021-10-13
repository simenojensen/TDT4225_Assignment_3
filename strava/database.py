import os
import pandas as pd
import numpy as np
from pymongo import MongoClient
import datetime

def parse_data():
    """Parse data from `.plt` files into Pandas DataFrames.

    Returns
    -------
    user_df : Pandas DataFrame
        Table of user information.
    activity_df : Pandas DataFrame
        Table of activity information.
    trackpoint_df : Pandas DataFrame
        Table of trackpoint information.
    """
    # Load user data into Pandas DataFrame
    user_ids = sorted(os.listdir("../dataset/Data/"))
    user_ids = [uid for uid in user_ids if not uid.startswith(".")] # remove Mac Finder generated files
    # Find labeled users
    with open("../dataset/labeled_ids.txt", "r") as f:
        labeled_users = f.read().splitlines()
    has_labels = [True if uid in labeled_users else False for uid in user_ids]
    user_df = pd.DataFrame({"id": user_ids, "has_labels": has_labels})

    # Activity and Trackpoint columns
    activity_cols = [
        "id",
        "user_id",
        "transportation_mode",
        "start_date_time",
        "end_date_time",
    ]
    trackpoint_cols = [
        "lat",
        "lon",
        "ignore",
        "altitude",
        "date_days",
        "date",
        "time"
    ]

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
    return (user_df, activity_df, trackpoint_df)


def insert_data(USER, PASSWORD, HOST, DB_NAME):
    """Function to create database, collections and insertion of data.

    Drops the `TDT4225ProjectGroup78` database if already exists, then creates
    the database and inserts the parsed data from the `.plt` files.

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

    user_df, activity_df, trackpoint_df = parse_data()

    uri = f"mongodb://{USER}:{PASSWORD}@{HOST}/{DB_NAME}"
    with MongoClient() as client:
        # Start by dropping the database.
        client.drop_database(DB_NAME)

        # Create database
        db = client[DB_NAME]

        # Create activity collection
        user_collection = db['user_collection']

        user_df = user_df.rename(columns={'id': '_id'})

        user_dict = user_df.head(10).to_dict("records")

        user_collection.insert_many(user_dict)
