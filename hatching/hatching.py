import pandas as pd
import numpy as np
import pymysql
from sqlalchemy import create_engine


def get_main_data (path):
    engine = create_engine('sqlite:////Users/Stefania/downloads/stefaniabraca.db')
    query = "select * from personal_info"
    dfpersonal_info = pd.read_sql_query(query, engine)
    query = "select * from business_info"
    dfbusiness_info = pd.read_sql_query(query, engine)
    query = "select * from business_info"
    dfbusiness_info = pd.read_sql_query(query, engine)
    merged_df = dfrank_info.merge(dfbusiness_info, on='id')
    final_df = merged_df.merge(dfpersonal_info, on='id')
    return final_df


def clean_col_final_df (final_df):
    # drop
    final_df.drop('Unnamed: 0_x', axis=1, inplace=True)
    final_df.drop('Unnamed: 0_y', axis=1, inplace=True)
    final_df.drop('Unnamed: 0', axis=1, inplace=True)
    final_df.drop('worthChange', axis=1, inplace=True)
    cols = list(final_df.columns.values)
    cols = ['id', 'name', 'lastName', 'position', 'worth', 'Source', 'age', 'gender', 'country', 'image']
    final_df = final_df.reindex(columns=cols)
    return final_df


def name_col (final_df):
    new_name = final_df["name"].str.split(" ", n=1, expand=True)
    final_df["First Name"] = new_name[0]
    final_df["Last Name"] = new_name[1]
    # Dropping old name column
    final_df.drop(columns=["name"], inplace=True)
    final_df.drop(columns=["Last Name"], inplace=True)
    for i in range(final_df.shape[0]):
        final_df.iat[i, 1] = final_df.iat[i, 1].strip().capitalize()
    cols_name = list(final_df.columns.values)
    #print cols_name
    final_df = final_df.reindex(columns=cols_name)
    print(final_df.head())

