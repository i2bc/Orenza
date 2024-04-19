#!/usr/bin/env python

import sqlite3
from datetime import datetime


def current_time():
    """
    Returns:
        The current hour, minutes and second
    """
    current_time = datetime.now()
    formatted_time = current_time.strftime("%H:%M:%S")
    return formatted_time


def create_connection(database: str):
    """
    Create a database connection to the SQLite database specified by db_file
    Parameters:
        database_path: path to the database file
    Returns:
        Connection object or None
    """
    con = None
    try:
        con = sqlite3.connect(database)
    except sqlite3.Error as e:
        print("Error connecting to database: ", e)

    return con
