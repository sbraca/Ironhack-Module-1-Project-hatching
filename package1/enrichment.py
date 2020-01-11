import requests
from bs4 import BeautifulSoup

#web scr top10
from package1.wrangle import final_df

url = 'https://en.wikipedia.org/wiki/The_World%27s_Billionaires'
html = requests.get(url).content
soup = BeautifulSoup(html, "lxml")


def scr_top10(url, html, soup):
    table = soup.find_all('table', {'class': 'wikitable sortable'})[0]
    return table


table = scr_top10(url, html, soup)


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

top10_df = scr_table(table)

top10_df = top10_df.astype({"Position": int})


def merge1 (df1,df2):
    merged_df = pd.merge(df1, df2, on='Position', how='outer')
    merged_df['Country'] = np.where(merged_df['Country'] == 'None', merged_df['Nationality'], merged_df['Country'])
    merged_df[['Country']] = merged_df[['Country']].fillna('None')
    return merged_df


merged_df = merge1(final_df, top10_df)

full_df = pd.read_csv('~/Desktop/projectonefolder/Ironhack-Module-1-Project-hatching/data/forbes_2018.csv')


def clean_fulldf (full_df):
    full_df.sort_values(by=['position'])
    null_displ = full_df[(full_df['position'].isnull()==True)]
    full_df.dropna(subset=['position'], inplace=True)
    full_df.position.astype(int)
    full_df.drop(columns=['lastName', 'age', 'gender', 'wealthSource', 'industry', 'worthChange', 'realTimeWorth', 'realTimePosition'], inplace=True)
    full_df.drop(columns=['worth', 'image'], inplace=True)
    full_df.rename(columns={"position": "Position"}, inplace=True)
    return full_df


full_df = clean_fulldf(full_df)


def merge2 (df1,df2):
    merged2_df = pd.merge(df1, df2, on='Position', how='outer')
    return merged2_df


merged2_df = merge2(merged_df, full_df)


def fillall (merged2_df):
    merged2_df['Country'] = np.where(merged2_df['Country'] == 'None', merged2_df['country'], merged2_df['Country'])
    return merged2_df


merged2_df = fillall(merged2_df)


def cleancountry (merged2_df):
    merged2_df['Country'] = merged2_df['Country'].str.replace('\xa0United States', 'United States')
    merged2_df['Country'] = merged2_df['Country'].str.replace("People's Republic of China", 'China')
    merged2_df['Country'] = merged2_df['Country'].str.replace('\xa0Spain', 'Spain')
    merged2_df["Country"]= merged2_df["Country"].astype(str)
    merged2_df.drop(columns=['Nationality', 'name','country'], inplace=True)
    return merged2_df


merged2_df = cleancountry(merged2_df)



