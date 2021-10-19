# -*- coding: utf-8 -*-
"""Code to perform queries on the `TDT4225ProjectGroup78` MongoDB database.

This module contains code that queries the `TDT4225ProjectGroup78` MongoDB
database, to answer the questions given in the assignment text. The results are
then printed to the console.
"""
import pandas as pd
import numpy as np
from haversine import haversine_vector, Unit
from tabulate import tabulate
from sklearn.cluster import DBSCAN
import pprint


def query_1(user, activity, trackpoint):
    """Find answers to question 1 by MongoDB queries.

    Results are printed to the console.

    Parameters
    ----------
    user : :obj:
        The pymongo collection object for user.
    activity : :obj:
        The pymongo collection object for activity.
    trackpoint : :obj:
        The pymongo collection object for trackpoint.
    """
    pprint.pprint(
        list(
            user.aggregate(
                [{"$group": {"_id": "Users", "NumberOfUsers": {"$count": {}}}}]
            )
        )
    )
    pprint.pprint(
        list(
            activity.aggregate(
                [
                    {
                        "$group": {
                            "_id": "Activities",
                            "NumberOfActivities": {"$count": {}},
                        }
                    }
                ]
            )
        )
    )
    pprint.pprint(
        list(
            trackpoint.aggregate(
                [
                    {
                        "$group": {
                            "_id": "Trackpoints",
                            "NumberOfTrackpoints": {"$count": {}},
                        }
                    }
                ]
            )
        )
    )

def query_2(user):
    """Find answers to question 2 by MongoDB queries.

    Results are printed to the console.

    Parameters
    ----------
    user : :obj:
        The pymongo collection object for user.
    """
    query = [
        {
            "$group": {
                "_id": "ActivitiesPerUser",
                "Average": {"$avg": {"$size": "$activity_id"}},
                "Minimum": {"$min": {"$size": "$activity_id"}},
                "Maximum": {"$max": {"$size": "$activity_id"}},
            }
        }
    ]
    pprint.pprint(list(user.aggregate(query)))

def query_3(user):
    """Find answers to question 3 by MongoDB queries.

    Results are printed to the console.

    Parameters
    ----------
    user : :obj:
        The pymongo collection object for user.
    """
    query = [
        {"$unwind": "$activity_id"},
        {"$group": {"_id": "$_id", "NumberOfActivities": {"$sum": 1}}},
        {"$sort": {"NumberOfActivities": -1}},
        {"$limit": 10},
    ]
    pprint.pprint(list(user.aggregate(query)))

def query_4(activity):
    """Find answers to question 4 by MongoDB queries.

    Results are printed to the console.

    Parameters
    ----------
    activity : :obj:
        The pymongo collection object for activity.
    """
    query = [
        {
            "$project": {
                "duration": {
                    "$dateDiff": {
                        "startDate": "$start_date_time",
                        "endDate": "$end_date_time",
                        "unit": "day",
                    }
                }
            }
        },
        {"$match": {"duration": {"$gt": 0}}},
        {
            "$lookup": {
                "from": "user",
                "localField": "_id",
                "foreignField": "activity_id",
                "as": "join_key",
            }
        },
        {"$group": {"_id": "$duration", "Users": {"$addToSet": "$join_key._id"},},},
        {
            "$project": {
                "_id": "UsersWithDifferentStartAndEndDate",
                "numberOfUsers": {"$size": "$Users"},
            }
        },
    ]
    pprint.pprint(list(activity.aggregate(query)))

def query_5(activity):
    """Find answers to question 5 by MongoDB queries.

    Results are printed to the console.

    Parameters
    ----------
    activity : :obj:
        The pymongo collection object for activity.
    """
    query = [
        {
            "$lookup": {
                "from": "user",
                "localField": "_id",
                "foreignField": "activity_id",
                "as": "join_key",
            }
        },
        {
            "$group": {
                "_id": {
                    "start_date_time": "$start_date_time",
                    "end_date_time": "$end_date_time",
                    "userId": "$join_key._id",
                },
                "activityIds": {"$addToSet": "$_id"},
                "count": {"$sum": 1},
            }
        },
        {"$match": {"count": {"$gt": 1}}},
    ]
    pprint.pprint(list(activity.aggregate(query)))

def query_6(user, trackpoint):
    """Find answers to question 6 by MongoDB queries.

    Results are printed to the console. Use DBSCAN to first cluster on users
    close in time. Then use DBSCAN again to cluster the results on users that
    are close in space.

    Parameters
    ----------
    user : :obj:
        The pymongo collection object for user.
    trackpoint : :obj:
        The pymongo collection object for trackpoint.
    """
    # Get data from user collection
    user_result = list(user.find({}, {"has_labels": 0}))
    user_df = (
        pd.DataFrame(user_result)
        .explode("activity_id", ignore_index=True)
        .rename(columns={"_id": "user_id"})
    )
    # Get data from trackpoint collection
    trackpoint_result = list(trackpoint.find({}, {"altitude": 0, "date_time": 0}))
    trackpoint_df = pd.DataFrame(trackpoint_result).rename(
        columns={"_id": "trackpoint_id"}
    )
    # Merge data from the user and trackpoint collection
    query_df = pd.merge(
        left=user_df, right=trackpoint_df, on="activity_id", how="right"
    )

    # Use DBSCAN to cluster on time
    X = (
        (query_df["date_days"] - query_df["date_days"].min()) * 24 * 60 * 60
    ).values.reshape(-1, 1)
    eps = 60  # seconds
    min_samples = 2
    cluster = DBSCAN(eps=eps, min_samples=min_samples).fit(X)
    query_df["time_labels"] = cluster.labels_
    query_df = query_df.loc[query_df["time_labels"] != -1]

    # Use DBSCAN again to cluster on distance using the haversine distance
    close_users = []
    for time_cluster in query_df["time_labels"].unique():
        df = query_df.loc[
            query_df["time_labels"] == time_cluster,
            ["user_id", "activity_id", "lat", "lon"],
        ]
        X = df[["lat", "lon"]].values
        eps = 100  # meters
        # divide by earth radius https://en.wikipedia.org/wiki/Earth_radius#Arithmetic_mean_radius
        eps = eps / 6371008.8
        min_samples = 2
        cluster = DBSCAN(eps=eps, min_samples=min_samples, metric="haversine").fit(X)
        df["spatial_labels"] = cluster.labels_
        df = df.loc[df["spatial_labels"] != -1]
        # Get sets of users that are close to each other
        df = df.groupby(["spatial_labels"]).agg(
            user_id=pd.NamedAgg(column="user_id", aggfunc=frozenset)
        )
        df = df.loc[
            df["user_id"].map(len) > 1
        ]  # must have minimum two users per spatial cluster
        # Only append unique sets for current iteration
        close_users.append(df["user_id"].unique())

    # Find all unique sets close users and remove empty arrays
    close_users = {s for arr in close_users if arr.size > 1 for s in arr}
    # Find total number of users that have been close
    number_of_close_users = len([user for s in close_users for user in s])
    print(f"Number of close users: {number_of_close_users}")

def query_7(user, activity):
    """Find answers to question 7 by MongoDB queries.

    Results are printed to the console.

    Parameters
    ----------
    user : :obj:
        The pymongo collection object for user.
    activity : :obj:
        The pymongo collection object for activity.
    """
    # Find all users who have taken a taxi
    query = [
        {
            "$lookup": {
                "from": "user",
                "localField": "_id",
                "foreignField": "activity_id",
                "as": "join_key",
            }
        },
        {"$match": {"transportation_mode": "taxi"}},
        {"$group": {"_id": "taxi", "taxiUserIds": {"$addToSet": "$join_key._id"},}},
    ]
    taxi_user_ids = list(activity.aggregate(query))[0]
    taxi_user_ids = [
        element for item in taxi_user_ids["taxiUserIds"] for element in item
    ]
    # Find all user ids
    all_user_ids = [item["_id"] for item in list(user.find({}, {"_id": 1}))]
    # Calculate the complement to get users who have never taken a taxi
    user_id_not_taxi = [i for i in all_user_ids if i not in taxi_user_ids]
    # Reformat for printing purposes
    values = np.array(user_id_not_taxi).reshape((-1, 4), order="F")
    cols = ["user_id"] * 4
    query_df = pd.DataFrame(data=values, columns=cols)
    print(tabulate(query_df, headers="keys", showindex=False, tablefmt="orgtbl"))

def query_8(activity):
    """Find answers to question 8 by MongoDB queries.

    Results are printed to the console.

    Parameters
    ----------
    activity : :obj:
        The pymongo collection object for activity.
    """
    query = [
        {
            "$lookup": {
                "from": "user",
                "localField": "_id",
                "foreignField": "activity_id",
                "as": "join_key",
            }
        },
        {
            "$group": {
                "_id": "$transportation_mode",
                "user_ids": {"$addToSet": "$join_key._id"},
            }
        },
        {"$match": {"_id": {"$ne": np.nan}}},
        {"$project": {"_id": "$_id", "myCount": {"$size": "$user_ids"}}},
    ]
    pprint.pprint(list(activity.aggregate(query)))

def query_9(user, activity):
    """Find answers to question 9 by MongoDB queries.

    Results are printed to the console. Use Pandas DataFrames to manipulate
    the data in order to find the relevant results.

    Parameters
    ----------
    user : :obj:
        The pymongo collection object for user.
    activity : :obj:
        The pymongo collection object for activity.
    """
    # Get data from user collection
    user_result = list(user.find({}, {"has_labels": 0}))
    user_df = (
        pd.DataFrame(user_result)
        .explode("activity_id", ignore_index=True)
        .rename(columns={"_id": "user_id"})
    )
    # Get data from activity collection
    activity_result = list(activity.find())
    activity_df = pd.DataFrame(activity_result).rename(columns={"_id": "activity_id"})
    # Merge data from the user and activity collection
    query_df = pd.merge(left=user_df, right=activity_df, on="activity_id", how="inner")

    # Create relevant time columns
    query_df["month_diff"] = query_df["end_date_time"].apply(
        lambda x: x.month
    ) - query_df["start_date_time"].apply(lambda x: x.month)
    query_df["year_month"] = (
        query_df["start_date_time"].apply(lambda x: x.year).astype(str)
        + "-"
        + query_df["start_date_time"].apply(lambda x: x.month_name())
    )
    query_df["recorded_hours"] = (
        query_df["end_date_time"] - query_df["start_date_time"]
    ).apply(lambda x: x.total_seconds() / 3600)

    # Find most active year-month
    year_month_ma = (
        query_df.groupby(["year_month"])
        .count()
        .sort_values(by="activity_id", ascending=False)
        .index[0]
    )
    # Find most active and second most active user in most active year-month
    ma_df = (
        query_df.loc[query_df["year_month"] == year_month_ma]
        .groupby(["user_id"])
        .count()
        .sort_values(by="activity_id", ascending=False)
    )
    user_ma_1 = ma_df.index[0]
    num_act_1 = ma_df["activity_id"][0]
    user_ma_2 = ma_df.index[1]
    num_act_2 = ma_df["activity_id"][1]

    # Select subset of data for most active and second most active user in
    # most active year-month
    query_df = query_df.loc[
        (query_df["year_month"] == year_month_ma)
        & (query_df["user_id"].isin([user_ma_1, user_ma_2]))
    ]

    # Assert that these users did not record any activities that started in
    # one month and ended in another
    assert all(query_df["month_diff"] == 0)

    # Create dataframe for number of activities and number of hours logged
    result_df = (
        query_df.groupby(["user_id"])
        .agg(recorded_hours=pd.NamedAgg(column="recorded_hours", aggfunc=sum))
        .reset_index()
    )
    d = {user_ma_1: num_act_1, user_ma_2: num_act_2}
    result_df["number_of_activities"] = result_df["user_id"].map(d)
    result_df["year_month"] = year_month_ma
    print(tabulate(result_df, headers="keys", showindex=False, tablefmt="orgtbl"))

def query_10(user, activity, trackpoint):
    """Find answers to question 1 by MongoDB queries.

    Results are printed to the console. Use Pandas DataFrames to sum the
    distance using the haversine Python package.

    Parameters
    ----------
    user : :obj:
        The pymongo collection object for user.
    activity : :obj:
        The pymongo collection object for activity.
    trackpoint : :obj:
        The pymongo collection object for trackpoint.

    """
    # Application side join to find relevant activities
    user = user.find_one({"_id": "112"})
    activities = activity.find(
        {"_id": {"$in": user["activity_id"]}, "transportation_mode": "walk"}
    )
    activity_ids = [item["_id"] for item in list(activities)]
    # Query trackpoint collection for relevant trackpoints
    trackpoints = trackpoint.aggregate(
        [
            {"$match": {"activity_id": {"$in": activity_ids}}},
            {
                "$project": {
                    "lat": "$lat",
                    "lon": "$lon",
                    "activity_id": "$activity_id",
                    "year": {"$year": "$date_time"},
                }
            },
            {"$match": {"year": 2008}},
        ]
    )
    query_df = pd.DataFrame(list(trackpoints))

    # Use the haversine Python package to get the total distance walked.
    distance_walked = 0
    for aid in query_df["activity_id"].unique():
        df = query_df.loc[query_df["activity_id"] == aid].copy()
        df["dist"] = haversine_vector(
            df[["lat", "lon"]].values,
            df[["lat", "lon"]].shift().values,
            Unit.KILOMETERS,
        )
        distance_walked += df["dist"].sum()
    print(f"Total distance walked: {distance_walked}")

def query_11(trackpoint):
    """Find answers to question 11 by MongoDB queries.

    Results are printed to the console.

    Parameters
    ----------
    trackpoint : :obj:
        The pymongo collection object for trackpoint.
    """
    query = [
        {
            "$setWindowFields": {
                "partitionBy": "$activity_id",
                "sortBy": {"_id": 1},
                "output": {
                    "shiftAltitude": {
                        "$shift": {"output": "$altitude", "by": 1, "default": np.nan}
                    }
                },
            }
        },
        {
            "$project": {
                "_id": 1,
                "activity_id": 1,
                "altitude": 1,
                "shiftAltitude": 1,
                "altitudeDiff": {"$subtract": ["$shiftAltitude", "$altitude"]},
            }
        },
        {"$match": {"altitudeDiff": {"$gt": 0}}},
        {
            "$group": {
                "_id": "$activity_id",
                "activityAltitudeGained": {"$sum": "$altitudeDiff"},
            }
        },
        {
            "$lookup": {
                "from": "user",
                "localField": "_id",
                "foreignField": "activity_id",
                "as": "user",
            }
        },
        {
            "$group": {
                "_id": "$user._id",
                "altitudeGained": {"$sum": "$activityAltitudeGained"},
            }
        },
        {"$sort": {"altitudeGained": -1}},
        {  # convert to feet
            "$project": {
                "_id": "$_id",
                "altitudeGained": {"$multiply": ["$altitudeGained", 0.3048]},
            }
        },
        {"$limit": 20},
    ]
    result = list(trackpoint.aggregate(query, allowDiskUse=True))
    pprint.pprint(result)

def query_12(trackpoint):
    """Find answers to question 12 by MongoDB queries.

    Results are printed to the console.

    Parameters
    ----------
    trackpoint : :obj:
        The pymongo collection object for trackpoint.
    """
    query = [
        {
            "$setWindowFields": {
                "partitionBy": "$activity_id",
                "sortBy": {"_id": 1},
                "output": {
                    "shiftDatetime": {
                        "$shift": {"output": "$date_time", "by": 1, "default": None}
                    }
                },
            }
        },
        {
            "$project": {
                "_id": 1,
                "activity_id": 1,
                "date_time": 1,
                "shiftDatetime": 1,
                "datetimeDiff": {"$subtract": ["$shiftDatetime", "$date_time"]},
            }
        },
        {"$match": {"datetimeDiff": {"$gt": 300000}}},  # 5 minutes in milliseconds
        {"$group": {"_id": "$activity_id"}},
        {
            "$lookup": {
                "from": "user",
                "localField": "_id",
                "foreignField": "activity_id",
                "as": "user",
            }
        },
        {"$group": {"_id": "$user._id", "numInvalidActivities": {"$count": {}},}},
        {"$sort": {"numInvalidActivities": -1}},
    ]
    result = list(trackpoint.aggregate(query, allowDiskUse=True))
    pprint.pprint(result)
