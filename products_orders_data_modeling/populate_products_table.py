"""
This script populates an Apache Cassandra table with product information
"""
import os
import csv

from cassandra_utilities import createCassandraConnection, createKeySpace
from query_utilities import execute_query

create_products_table_query = """CREATE TABLE IF NOT EXISTS products(
                            vendor text,
                            product_name text,
                            image_url text,
                            category text,
                            price float,
                            PRIMARY KEY(vendor, product_name))
                            """
insert_product_query = """INSERT INTO products(vendor, product_name, image_url, category, price) VALUES (%s, %s, %s, %s,%s);"""


def populate_products_table():
    dbsession = createCassandraConnection()
    createKeySpace("ks1", dbsession)
    try:
        dbsession.set_keyspace('ks1')
    except Exception as e:
        print(e)
    execute_query(create_products_table_query, dbsession)

    # Create a list of .csv files
    CSV_DIRECTORY = 'data/products'
    csv_files = []
    for file in os.listdir(CSV_DIRECTORY):
        file_path = 'data/products/{}'.format(file)
        if file_path.split('.')[-1] == 'csv':
            csv_files.append(file_path)

    # For each .csv file add all products
    for file in csv_files:
        with open(file, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                dbsession.execute(insert_product_query, [row['vendor'], row['name'], row['url'], row['category'], float(row['price'])])


if __name__ == '__main__':
    populate_products_table()