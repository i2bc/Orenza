#!/usr/bin/env python

import pickle
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


def save_pickle(data: dict, output_file: str):
    """
    This function stores a dictionary of parsed data as a pickle file.
    Args:
        data : a dictionary containing the data
        output_file : name and path of the output file
        path : the path where you want the file to be stored
    """
    try:
        with open(output_file, "wb") as f:
            pickle.dump(data, f)
    except Exception as e:
        print(f"Error occurred while saving pickle file: {e}")


def load_pickle(input_file: str):
    """
    This function loads a pickle file.
    Args:
        input_file: Name and path of the pickle file.
    Returns:
        The data loaded from the pickle file.
    """
    try:
        with open(input_file, "rb") as f:
            data = pickle.load(f)
        return data
    except Exception as e:
        print(f"Error occurred while loading pickle file: {e}")
        return None
