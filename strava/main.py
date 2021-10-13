# -*- coding: utf-8 -*-
"""
TDT4225 Very Large, Distributed Data Volumes - Assignment 3
Author: Simen Omholt-Jensen

This module contains code that runs the strava interface.
"""
import sys
import getpass

import database

import importlib
importlib.reload(database)


from database import insert_data




def main():
    """Sets up the database and runs the program.

    Program prompts user for their MongoDB login information. A database called
    `TDT4225ProjectGroup78` is created and filled with data from the `.plt`
    files in the `dataset` folder. The program then queries the database to
    answer the questions found in the assignment text.

    """

    # Prompt the user for their MongoDB login inforamtion
    # USER = input("Enter MongoDB user: ")
    USER='root'
    # PASSWORD = getpass.getpass(prompt="Enter MongoDB password: ")
    PASSWORD = "q5fP6kN4A!Pk#I8Q$Tn"

    # Database name
    DB_NAME = "TDT4225ProjectGroup78"

    # Host name
    HOST = "localhost"

    # create strava database
    insert_data(USER, PASSWORD, HOST,  DB_NAME)



if __name__ == "__main__":
    main()
