import pandas as pd
import numpy as np
import common_functions
# Remember to use df.[mask] way of selecting items when we dont need to filter by rows / index .

data = {
    'Population': [35.467, 63.951, 80.94, 60.665, 127.061, 64.511, 318.523],
    'GDP': [
        1785387,
        2833687,
        3874437,
        2167744,
        4602367,
        2950039,
        17348075
    ],
    'Surface Area': [
        9984670,
        640679,
        357114,
        301336,
        377930,
        242495,
        9525067
    ],
    'HDI': [
        0.913,
        0.888,
        0.916,
        0.873,
        0.891,
        0.907,
        0.915
    ],
    'Continent': [
        'America',
        'Europe',
        'Europe',
        'Europe',
        'Asia',
        'Europe',
        'America'
    ]
}

# common_functions.sort_dictionary(data)
index = ['Canada',
         'France',
         'Germany',
         'Italy',
         'Japan',
         'United Kingdom',
         'United States']
col_names = ['Population', 'GDP', 'Surface Area', 'HDI', 'Continent']
df = pd.DataFrame(data, index=index, columns=col_names)

print(df)

# 1. accessing items

single_col = df["Population"]
multiple_cols = df[["Population", "GDP", "Surface Area"]]
print(f"single_col \n{single_col}")
print(f"multiple_cols \n{multiple_cols}")

# 2. SQLs
# select * from table where Population > 70 and GDP > 2833687
mask = (df["Population"] > 70) & (df["GDP"] > 2833687)
sql_1 = df[mask]
print(sql_1)

mask  = df["Continent"].str.lower() == "america"
print(df[mask])

max_gdp = df["GDP"].max()
print(max_gdp)
mask = df["GDP"] == max_gdp
df[mask]

# select * from table where index_name = Canada
# Remember to access rows by index we must need to use .loc[] / .iloc[]
sql_2 = df.loc["Germany"]
print(sql_2)

sql_3 = df.loc[["Germany", "France"]]
print(sql_3)

# 3. range access from and to
col_range_1 = df.loc[:,"Population" : "HDI"]
print(col_range_1)
row_range_1 = df.loc["France" : "Japan"]
print(row_range_1)

row_range_2= df.loc["Japan":"Canada":-1]

# 4. drop rows
d1 = df.drop(['Italy', 'Canada'], axis=0 )
# 5. drop cols
d2 = df.drop(['Population', 'HDI'], axis=1)


# 5. col operation with series
s = pd.Series([100,200])
s.index = ["GDP" , "Surface Area"]
print(s)
column_operaton = df[["GDP","Surface Area"]] + s
print(column_operaton)

# 6. added new col, remember the index need to be same as corresponding index/row names of the DF
# and use df["new_col"]

lang = pd.Series(["French" , "German"], index=["France","Germany"])

df["Language"] = lang
print(df)

df["is_high_popupalated"] = df["Population"] > 70
print(df)

df["Density(P/A)"] = df["Population"] / df["Surface Area"]
print(df)

# 7. add new rows , remember the index need to be same as corresponding col names
# and use df.loc[]

s=  pd.Series([2.0,100],index=["Population" , "GDP"])
df.loc["India"] = s
print(df)
