import pandas as pd
import numpy as np
import pymysql
from sqlalchemy import create_engine
import pymysql
from sqlalchemy import create_engine



def get_main_data():
    engine = create_engine('sqlite:////Users/Stefania/downloads/stefaniabraca.db')
    query = "select * from personal_info"
    dfpersonal_info = pd.read_sql_query(query, engine)
    query = "select * from business_info"
    dfbusiness_info = pd.read_sql_query(query, engine)
    query = "select * from rank_info"
    dfrank_info = pd.read_sql_query(query, engine)
    merged_df = df1.merge(df2, on='id')
    raw_df = merged_df.merge(df3, on='id')
    return raw_df

raw_df = get_main_data(dfrank_info, dfbusiness_info, dfpersonal_info)
print(raw_df)