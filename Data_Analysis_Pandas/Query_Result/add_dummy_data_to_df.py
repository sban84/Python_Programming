import pandas as pd

# way 1 of creating DF
data = pd.DataFrame(
    {
        "DATE": ["2010-01-01", "2020-01-01"],
        "TAG":  ["C#" , "scala"],
        "POST": [100 , 0]
    }
)
print(data)

# way 2 ,
data_1 =  pd.DataFrame(
    [ ["2010-01-01", "java", 1 ],
     ["2015-01-01", "python", 1],
     ["2018-01-01", "c", 1]]
    , columns=["DATE", "TAG", "POST"]
)

merged_df = data_1.append(data)
print(merged_df)

print(merged_df["TAG"])

filtered = merged_df.loc[merged_df["TAG"].isin(["scala", "java"])]
print(filtered)

