# This is a sample Python script.

# Press ⇧F10 to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import lithops
from lithops import Storage
import pandas as pd
import pandasql as psql
from lithops.storage.cloud_proxy import os, open
import ibmcloudsql

def HighestValue(file):
    csvfile = pd.read_csv(file)
    query = """ SELECT MAX(c.High)
                FROM file c 
                WHERE Open!='NaN'"""
    return psql.sqldf(query)

def LowestValue():
    my_instance_crn="crn:v1:bluemix:public:sql-query:eu-de:a/cbf04fddca8543bfa4372e8f723629de:17f0c671-2133-4785-8bb4-d9316602dbb8::"
    my_ibmcloud_apikey="luuI-qUPig0gbEGteeE_4jaW8AIOaFxqXVoN7fpfsfiK"
    my_target_cos_url="cos://eu-de/sdprac2python/bitstampUSD_1-min_data_2012-01-01_to_2021-03-31-000.csv"
    sqlClient = ibmcloudsql.SQLQuery(my_ibmcloud_apikey,my_instance_crn)
    #csvfile = pd.read_csv(file)
    query = """ SELECT MIN(c.Low)
                FROM csvfile c 
                WHERE Open!='NaN'"""

    query2 = "SELECT MIN(c.Low) FROM " + my_target_cos_url + " WHERE Open!='NaN' "
    return sqlClient.run_sql(query2)
    #return psql.sqldf(query)



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    storage = Storage()
    #fexec = lithops.FunctionExecutor()
    #with open("bitstampUSD_1-min_data_2012-01-01_to_2021-03-31-000.csv", 'r') as f:
        #fexec.call_async(LowestValue, f)
        #print(fexec.get_result())
    print(LowestValue())



"""

"""
