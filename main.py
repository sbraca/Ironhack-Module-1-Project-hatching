import pandas as pd
import numpy as np
import pymysql
from sqlalchemy import create_engine
import requests
from bs4 import BeautifulSoup



#get raw data#
engine = create_engine('sqlite:////Users/stefaniabraca/Ironhack-Module-1-Project-hatching/data/stefaniabraca.db')
query = "select * from personal_info"
dfpersonal_info = pd.read_sql_query(query, engine)
query = "select * from business_info"
dfbusiness_info = pd.read_sql_query(query, engine)
query = "select * from rank_info"
dfrank_info = pd.read_sql_query(query, engine)

forbes_df = pd.read_csv('~/Ironhack-Module-1-Project-hatching/data/forbes_2018.csv')
tertiary_ed = pd.read_csv('~/Ironhack-Module-1-Project-hatching/data/tertiary_ed.csv')

#data wrangling#
col_todrop = [ 'worthChange','lastName', 'Unnamed: 0_x', 'Unnamed: 0_y', 'realTimeWorth', 'realTimePosition', 'Unnamed: 0', 'image']
reindex_col = ['position', 'worth', 'name', 'age', 'gender', 'Source', 'country']

#web scrapping#
url = 'https://en.wikipedia.org/wiki/The_World%27s_Billionaires'
html = requests.get(url).content
soup = BeautifulSoup(html, "lxml")

########  acquire  ########

def get_main_data(df1, df2, df3):

    merged_df = df1.merge(df2, on='id')
    raw_df = merged_df.merge(df3, on='id')
    return raw_df

########  wrangling  ########

def drop(df, list_col_todrop):
    drop
    df.drop(list_col_todrop, axis=1, inplace=True)
    cols = list(df.columns.values)
    cols = ['id', 'name', 'position', 'Source', 'worth', 'age', 'gender', 'country']
    df = df.reindex(columns=cols)
    df.name = df.name.str.title()
    return df

def reindex (df, listt):
    df = df.reindex(columns=listt)
    newlist = [w.title() for w in listt]
    df.columns = newlist
    return df

def gender (df, index1, index2):
    df.loc[index1, 'Gender']='Female'
    df.loc[index2, 'Gender']='Female'
    df["Gender"].fillna("Male", inplace = True)
    df.astype({'Gender': str}).dtypes
    return df

def gender_cat (df):
    df.loc[df['Gender'].str.startswith('M'), 'Gender_cat'] = 'Male'
    df.loc[df['Gender'].str.startswith('F'), 'Gender_cat'] = 'Female'
    df[['Gender_cat']] = df[['Gender_cat']].fillna('Not Available')
    df.drop('Gender', axis=1, inplace=True)
    return df



def year_toage(df):
    df['Age'] = df['Age'].str.replace('years old', '')
    df['Age'] = df['Age'].str.replace('1982', '36')
    df['Age'] = df['Age'].str.replace('1983', '35')
    df['Age'] = df['Age'].str.replace('1984', '34')
    df['Age'] = df['Age'].str.replace('1985', '33')
    df['Age'] = df['Age'].str.replace('1986', '32')
    df['Age'] = df['Age'].str.replace('1987', '31')
    df['Age'] = df['Age'].str.replace('1988', '30')
    df['Age'] = df['Age'].str.replace('1989', '29')
    df['Age'] = df['Age'].str.replace('1990', '28')
    df['Age'] = df['Age'].str.replace('1991', '27')
    df['Age'] = df['Age'].str.replace('1992', '26')
    df['Age'] = df['Age'].str.replace('1994', '24')
    df['Age'] = df['Age'].str.replace('1996', '22')
    df['Age'] = df['Age'].str.replace('1998', '20')
    return df

def nan_toage (df):
    df[['Age']] = df[['Age']].fillna(0)
    df.Age = df.Age.astype(int)
    mean= df['Age'].mean()
    df['Age'] = np.where(df['Age'] == 0, df.Age.replace(0,'mean'), df['Age'])
    return df

def clean_country (df):
    df.Country = df.Country.astype(str)
    df['Country'] = df['Country'].str.replace('USA', 'United States')
    df['Country'] = df['Country'].str.replace("People's Republic of China", 'China')
    return df

########  enrichment   ########

def scr_top10 (url, html, soup):
    table = soup.find_all('table',{'class':'wikitable sortable'})[0]
    return table


def scr_table(table):
    rows = table.find_all('tr')
    rows_parsed = [row.text for row in rows]
    rows = [row.text.strip().split("\n") for row in rows]
    colnames = rows[0]
    for ele in colnames:
        if ele == (''):
            colnames.remove(ele)
    data = rows[1:]
    top10_df = pd.DataFrame(data, columns=colnames)
    top10_df.drop(columns=['Name', 'Net worth (USD)', 'Age', 'Source(s) of wealth'], inplace=True)
    top10_df.rename(columns={"No.": "Position"}, inplace=True)
    return top10_df

def merge1 (df1,df2):
    df1.Position = df1.Position.astype(int)
    merged_df = pd.merge(df1, df2, on='Position', how='outer')
    merged_df['Country'] = np.where(merged_df['Country'] == 'None', merged_df['Nationality'], merged_df['Country'])
    merged_df[['Country']] = merged_df[['Country']].fillna('None')
    return merged_df

def clean_forbesdf (df):
    df.sort_values(by=['position'])
    null_displ = df[(df['position'].isnull()==True)]
    df.dropna(subset=['position'], inplace=True)
    df.position.astype(int)
    df.drop(columns=['lastName', 'age', 'gender', 'wealthSource', 'industry', 'worthChange', 'realTimeWorth', 'realTimePosition'], inplace=True)
    df.drop(columns=['worth', 'image'], inplace=True)
    df.rename(columns={"position": "Position"}, inplace=True)
    return df


def merge2 (df1,df2):
    merged2_df = pd.merge(df1, df2, on='Position', how='outer')
    merged2_df['Country'] = np.where(merged2_df['Country'] == 'None', merged2_df['country'], merged2_df['Country'])
    return merged2_df

def cleancountry (merged2_df):
    merged2_df['Country'] = merged2_df['Country'].str.replace('\xa0United States', 'United States')
    merged2_df['Country'] = merged2_df['Country'].str.replace("People's Republic of China", 'China')
    merged2_df['Country'] = merged2_df['Country'].str.replace('\xa0Spain', 'Spain')
    merged2_df["Country"]= merged2_df["Country"].astype(str)
    merged2_df.drop(columns=['Nationality', 'name','country'], inplace=True)
    return merged2_df


#########  PIPELINE   ########
def main ():
    raw_df = get_main_data(dfpersonal_info, dfbusiness_info, dfrank_info)
    final_df = drop(raw_df, col_todrop)
    final_df.name = final_df.name.str.title()
    final_df = reindex(final_df, reindex_col)
    final_df = gender(final_df, 26, 596)
    final_df = gender_cat(final_df)
    final_df = year_toage(final_df)
    final_df = nan_toage(final_df)
    final_df.Country = final_df.Country.astype(str)
    wrangled_df = clean_country(final_df)
    table = scr_top10(url, html, soup)
    top10_df  = scr_table(table)
    top10_df = top10_df.astype({"Position": int})
    merged_df = merge1(wrangled_df, top10_df)
    clean_forbes_df = clean_forbesdf(forbes_df)
    merged2_df = merge2(merged_df, clean_forbes_df)
    merged2_df = cleancountry(merged2_df)
    print(merged2_df)

if __name__ == "__main__":
    main()