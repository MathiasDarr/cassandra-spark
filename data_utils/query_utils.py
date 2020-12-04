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

def insert_statement(file, query, insert_dict, session):
    """
    This function reads csv file, extracts certain values from
    file by executing query, and casts certain data type of each
    column of data in line

    Parameters:
        file: file name ended with .csv  E.g. filename = "data123.csv"
        query: CQL query with INSERT statement
        insert_dict: Python dictionary format,
                     Key: column index in csv file
                     Value: casted type
                     E.g. dict = {2: int} which refers to the third column of data
                                 to be casted by Integer
    """
    with open(file, encoding='utf8') as f:
        csvreader = csv.reader(f)  # create reader object
        next(csvreader)  # skip header
        for line in csvreader:
            session.execute(query, [y(line[x]) for x, y in insert_dict.items()])


def query_to_df(query, session):
    """
    This function will execute the SELECT query
    and return the result as DataFrame format

    Parameter:
        query: CQL query with SELECT statement

    """

    def pandas_factory(colnames, rows):
        """
        Inner function to return the pandas DataFrame with
        paramter rows and columns corresponding to the function
        parameters. The function asks Cassandra to transform
        all the rows in data into DataFrame format and column
        names equal to the original colnames
        """
        return pd.DataFrame(rows, columns=colnames)

    # Assign the inner function to row_factory, return all the rows
    # into the DataFrame format
    session.row_factory = pandas_factory
    # Execute the query
    result = session.execute(query, timeout=None)
    # return the dataframe of all the rows
    df = result._current_rows
    return df