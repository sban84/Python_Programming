import pandas as pd

import matplotlib.pyplot as plt


pd.set_option('display.width', 400)
pd.set_option('display.max_columns', 12)
# data = pd.read_csv("../../data/sets.csv" )
# print(data.head())
#
# print(data.count())
#print(data.loc[:, "set_num"])

# Challenge 1 , get the unique colours
# color = pd.read_csv("../../data/colors.csv")
# print(color.head(5))
# uniq_name_count = len(color["name"].unique())
# print(uniq_name_count)
# print(color["name"].nunique())
# # this wll give the each col and its count in series format
# print(color["name"].value_counts())

## Challenge 2 # find the count for is_trans = t and =f separately
#
# t_cols = color.loc[ color["is_trans"] == "t", :]
# print(t_cols.count()["id"])
# f_cols = color.loc[ color["is_trans"] == "f"]
# print(f_cols.count()["id"])
#
# print(color["is_trans"].unique())
#
# using_grp = color.groupby(by="is_trans").count()
# print(using_grp)
# using_grp_filterd = using_grp.loc["f"]["id"]
# print(using_grp_filterd)

## Challenge 3 # find the In which year were the first LEGO sets released and what were these sets called?
sets = pd.read_csv("../../data/sets.csv" )
print(sets.head())
sets_sorted_by_year = sets.sort_values("year", ascending=True)

#print(sets_sorted_by_year.head(10))
## find out how many different products the company was selling in their first year since launch:
get_first_year = sets_sorted_by_year.head(1).loc[:, "year"]
print(get_first_year.values[0])
filter_by_first_year = sets_sorted_by_year.loc[ sets_sorted_by_year["year"] == get_first_year.values[0]]
print(filter_by_first_year)
no_product_in_first_year =filter_by_first_year["set_num"].nunique()
print(no_product_in_first_year)
names_of_these_prod = filter_by_first_year["set_num"]
print(names_of_these_prod)


## Challenge 4 : How many different products did the LEGO company sell in their first year of operation?
# no_prod_first_year = first_year_set["set_num"].nunique()
# print("no_prod_first_year" , no_prod_first_year)

## Challenge 5 : What are the top 5 LEGO sets with the most number of parts?

# top_sets = sets.sort_values("num_parts" , ascending= False)
# print("top LEGO with highest parts " , top_sets.head(5))


# Challenge 6 , sets/year
sets_yearly = sets.groupby("year").count()
print(sets_yearly.loc[:,"set_num"])

plt.plot(sets_yearly.index , sets_yearly.set_num)

## Challenge 7 : we want to calculate the number of different themes by calendar year. This means we have to group the data by year and then count the number of unique theme_ids for that year.

unique_themed_per_year = sets.groupby(by="year").agg({ "theme_id" : pd.Series.nunique })
print(unique_themed_per_year)
filterd = unique_themed_per_year.iloc[:-2]
print(filterd)
plt.plot(filterd.index , filterd["theme_id"].iloc[:-2])