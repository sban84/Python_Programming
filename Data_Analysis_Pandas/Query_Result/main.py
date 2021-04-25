import pandas as pd
import datetime as dt

from numpy import NaN

data = pd.read_csv("../../data/QueryResults.csv", names=["DATE", "TAG", "POST"], header=0)

print(data.head(10))
# row_count = data.shape[0]
# col_count = data.shape[1]
# # print(f"row_count = {row_count} and col count = {col_count}")
#
# any_col_null_check = [col for col in data.columns if data[col].isnull().any()]
# # print(any_col_null_check)
#
# null_post_records = data.loc[data["POST"].isnull()]
# print(null_post_records)

#index_of_null_post_records = data.loc[data["POST"].isnull()].index.to_list()
# print(index_of_null_post_records)
# #To count the number of entries in each column we can use .count(). Note that .count() will actually tell us the
# number of non-NaN values in each column.
# print(data.count())

# Challenge 1 , The TAG is the name of the programming language. So for example in July 2008, there were 3 posts tagged with the language C#. Given that the TAG serves as our category column, can you figure out how to count the number of posts per language? Which programming language had the most number of posts since the creation of Stack Overflow?

calculate_numof_post_by_tag = data.groupby(by="TAG").sum()
print(calculate_numof_post_by_tag.head(10))

sort_based_on_count = calculate_numof_post_by_tag.sort_values(by="POST", ascending=False)
print(sort_based_on_count.head(5))


# Challenge 2 , For example to get for year ,
# print(data)
# data_2008 = data.loc[data["DATE"] == "2008-08-01 00:00:00" ]
# print(type(data_2008))
#
# year_2008_data= data.loc[ data["DATE"].str.contains("2009") , : ]
# #print(year_2008_data)
#
# # data["count"] =  count_by_numof_tags.values
# # print(data.head(10))
#


# # <--- Challenge 3 , group by month for year = 2008 ---->
# def convert_to_date(s):
#
#     # 2008-07-01 00:00:00
#     date_obj = dt.datetime.strptime(s , "%Y-%m-%d %H:%M:%S")
#     print(date_obj)
#     return date_obj
#
#
# #convert_to_date("2009-12-01 00:00:00")
#
# data["date_obj"] =  data["DATE"].apply(convert_to_date)
# print(data)
# data['month'] = data['date_obj'].apply(lambda x: x.strftime('%m'))
# data['year'] = data['date_obj'].apply(lambda x: x.strftime('%Y'))
# print(data.head(5))
#
## get_year_from_DATE_col = data["DATE"].apply(lambda x : dt.datetime.strptime(x,"%Y-%m-%d %H:%M:%S").strftime("%Y"))
# print(get_year_from_DATE_col)

# result = data.groupby('DATE','TAG','POST').count().reset_index()
#
# #result = data.groupby(["POST"])["DATE","TAG" , "POST"].transform("count")
# print(result)
#
#
# #raw_data['Mycol'] =  pd.to_datetime(raw_data['Mycol'], format='%d%b%Y:%H:%M:%S.%f')
#
#
# # year_2008_data_filtered = data.loc[ convert_to_date(data["DATE"].str).year  == "2008"]
# # print(year_2008_data_filtered)
#
#
# Challenge 4 # another way to conver string col representing date to actual datetime object

# data["date_obj"] = pd.to_datetime(data["DATE"])
# print(type(data["date_obj"][2])) ## <class 'pandas._libs.tslibs.timestamps.Timestamp'>
# print(data.head(3))


# Challenge 5 , working with null / NaN values in DF ( NOTE :: this is IMPORTANT )

# data.loc[len(data)] = [NaN, "aa", 1]
# print(data["TAG"].unique())
#
# null_date_rec =  data.loc[ data["DATE"].isnull()]
# print(null_date_rec["DATE"])
#
# data.fillna("2000-01-01" , inplace=True)
# null_date_rec =  data.loc[ data["DATE"].isnull()]
# print(null_date_rec["DATE"])
# print(data.loc[ data["DATE"] == "2000-01-01" , :])
# indx = data.loc[ data["DATE"] == "2000-01-01" , :]
# print(indx.loc[1991]) # will return the series of the row = 1991 index
#
# # checking overall df any col has null
# print(data.isnull().any()) # will return series with col details
# print(data.isnull().values.any()) # will return T/F if any val is null ?
#
#



