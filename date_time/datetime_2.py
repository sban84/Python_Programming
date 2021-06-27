from datetime import datetime
from dateutil.relativedelta import relativedelta

d1 = datetime(2019, 1, 28, 10, 12, 00)
d2 = datetime.now()
print(f" d1 = {d1} and d2 = {d2}")
# 1.
print(f"only days diff {(d2 - d1).days}")  # best way to get days diff

# 2.
exact_diff = relativedelta(d2, d1)
print(f"exact_diff = {exact_diff}")

# 3.
months_diff = exact_diff.months + (12 * exact_diff.years)

if months_diff > 0:  # as starts with 0 for 1 mon diff
    months_diff += 1
print(f"finally months diff = {months_diff}")

# 4. Year diff
year_diff = exact_diff.years
print(f"year diff = {year_diff}")

# 5. get the date from a given date by adding / subs mont /  year / days

after_adding_n_months = d2 + relativedelta(months=13)
print(f"after_adding_n_months = {after_adding_n_months}")

after_adding_n_years = d2 + relativedelta(years=13)
print(f"after_adding_n_years = {after_adding_n_years}")

after_subs_n_months = d2 - relativedelta(months=13)
print(f"after_subs_n_months = {after_subs_n_months}")

after_subs_n_years = d2 - relativedelta(years=13)
print(f"after_subs_n_years = {after_subs_n_years}")

after_adding_n_days = d2 + relativedelta(days=13)
print(f"after_adding_n_days = {after_adding_n_days}")

after_subs_n_days = d2 - relativedelta(days=13)
print(f"after_subs_n_days = {after_subs_n_days}")

after_adding_n_hrs = d2 + relativedelta(hours=13)
print(f"after_adding_n_hrs = {after_adding_n_hrs}")

# 6. get the no of weekdays between 2 dates

""" s1 startdate , s2 end date in Strng format """


def get_no_weekdays(s1, s2):
    d1 = datetime.strptime(s1, "%Y-%m-%d %H:%M:%S")
    d2 = datetime.strptime(s2, "%Y-%m-%d %H:%M:%S")
    count_weekdays = 0
    while d1 < d2:
        print("aaa")
        if d1.weekday() in range(0, 5):
            print("weekday ", d1.weekday())
            count_weekdays += 1
        else:
            print("weekend ", d1.weekday())
        d1 = d1 + relativedelta(days=1)
    print(count_weekdays)


get_no_weekdays("2021-4-20 10:12:00", "2021-05-01 00:48:44")

# 3. find the day of date

week_map = {
    "1": "Sunday",
    "2": "Monday",
    "3": "Tuesday",
    "4": "Wednesday",
    "5": "Thursday",
    "6": "Friday",
    "7": "Saturday"}

date = datetime.strptime("08 05 2015", "%d %m %Y")
print(week_map.get(str(date.weekday())  ) )

