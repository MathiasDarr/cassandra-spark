import csv
import pandas as pd


def execute_query(query, session):
    """
    This function will try to execute the query passed by function parameter, or
    print exception if execution of CQL query fails
    Parameter:
        query: CQL query to be executed
    """
    try:
        session.execute(query)
    except Exception as e:
        print(e)