col_todrop = ['worthChange', 'Unnamed: 0_x', 'Unnamed: 0_y', 'realTimeWorth', 'realTimePosition', 'Unnamed: 0',
                  'image']

def drop(df, list_col_todrop):
    # drop
    col_todrop = ['worthChange', 'Unnamed: 0_x', 'Unnamed: 0_y', 'realTimeWorth', 'realTimePosition', 'Unnamed: 0',
                  'image']
    df.drop(list_col_todrop, axis=1, inplace=True)
    return df


final_df = drop(final_df, col_todrop)

def new_name (df):
    col=df["name"].str.split(" ", n = 1, expand = True)
    df["First Name"]= col[0]
    df["Last Name"]= col[1]
    return df

final_df = new_name(final_df)


def format_data(df):
    for i in range(df.shape[0]):
        df.iat[i, 1] = df.iat[i, 1].strip().capitalize()
        df.iat[i, 5] = df.iat[i, 5].strip().capitalize()
        df.iat[i, 9] = df.iat[i, 9].strip().capitalize()
    return df


final_df = format_data(final_df)

name_drop = ['name', 'Last Name', 'id' ]
final_df = drop(final_df, name_drop)

reindex_col = ['position', 'worth', 'First Name', 'lastName', 'age', 'gender', 'Source', 'country']
def reindex (df, list):
    df = df.reindex(columns=list)
    newlist = [w.title() for w in list]
    df.columns = newlist
    return df
final_df = reindex(final_df, reindex_col)

null_displ = final_df[(final_df['Gender'].isnull()==True)]

def gender (df):
    df.loc[26, 'Gender']='Female'
    df.loc[596, 'Gender']='Female'
    df["Gender"].fillna("Male", inplace = True)
    df.astype({'Gender': str}).dtypes
    return df

final_df = gender(final_df)


def gender_cat(df):
    df.loc[df['Gender'].str.startswith('M'), 'Gender_cat'] = 'Male'
    df.loc[df['Gender'].str.startswith('F'), 'Gender_cat'] = 'Female'
    df[['Gender_cat']] = df[['Gender_cat']].fillna('Not Available')
    df.drop('Gender', axis=1, inplace=True)

    return df

final_df = gender_cat(final_df)

final_df.Position = final_df.Position.astype(int)


def clean_age(df):
    df = df['Age'].str.replace('years old', '')
    df[['Age']] = df[['Age']].fillna(0)
    df['Age'] = df['Age'].str.replace('years old', '')
    df[['Age']] = df[['Age']].fillna(0)
    return df

final_df = clean_age(final_df)

def year_toage (df):
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

final_df = year_toage (final_df)


def nan_toage(df):
    df[['Age']] = df[['Age']].fillna(0)
    df.Age = df.Age.astype(int)
    mean = df['Age'].mean()
    df.replace({'Age': {0: 'mean'}})
    return df

final_df = nan_toage(final_df)

final_df.Country = final_df.Country.astype(str)


def clean_country(df):
    df['Country'] = df['Country'].str.replace('USA', 'United States')
    df['Country'] = df['Country'].str.replace("People's Republic of China", 'China')
    return df

cleaned_final_df = clean_country(final_df)
