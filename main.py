# This is a sample Python script.

# Press ⇧F10 to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

from lithops import Storage
import pandas as pd
import pandasql as psql
from lithops.storage.cloud_proxy import os, open

def HighestValue(file):
    csvfile = pd.read_csv(file)
    query = """ SELECT MAX(c.High)
                FROM file c 
                WHERE Open!='NaN'"""
    print(psql.sqldf(query))

def LowestValue(file):
    csvfile = pd.read_csv(file)
    query = """ SELECT MIN(c.Low)
                FROM csvfile c 
                WHERE Open!='NaN'"""
    print(psql.sqldf(query))



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    storage = Storage()
    with open("bitstampUSD_1-min_data_2012-01-01_to_2021-03-31.csv", 'r') as f:
        LowestValue(f)


"""

"""
