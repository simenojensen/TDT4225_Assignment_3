# -*- coding: utf-8 -*-
"""Run program.

TDT4225 Very Large, Distributed Data Volumes - Assignment 3.
Author: Simen Omholt-Jensen

This module contains code that runs the strava interface.
"""
import getpass
from database import insert_data
from database import query_database
from database import create_user

def main():
    """Set up the database and run the program.

    Program prompts user for their MongoDB login information. A database called
    `TDT4225ProjectGroup78` is created with the user login information, and
    filled with data from the `.plt` files in the `dataset` folder. The program
    then queries the database to answer the questions found in the assignment
    text.

    """
    # Prompt the user for their MongoDB login inforamtion
    USER = input("Enter MongoDB user: ")
    PASSWORD = getpass.getpass(prompt="Enter MongoDB password: ")

    # Database name
    DB_NAME = "TDT4225ProjectGroup78"

    # Host name
    HOST = "localhost"

    # Create user
    create_user(USER, PASSWORD, HOST,  DB_NAME)

    # create strava database
    insert_data(USER, PASSWORD, HOST,  DB_NAME)

    # Perform queries
    query_database(USER, PASSWORD, HOST,  DB_NAME)

if __name__ == "__main__":
    main()
