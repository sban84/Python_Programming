import pandas as pd

data = pd.read_csv("../../data/QueryResults.csv" , names= ["Date" , "Tag" , "Post"])
print(data.head(3))

## filtering .....
python_tag_data= data.loc[ data["Tag"] == "python"]
#print(python_tag_data)

year_2008_data= data.loc[ data["Date"].str.contains("2009") , : ]
print(year_2008_data)