from datetime import datetime
from dateutil.relativedelta import relativedelta

week_map = {
    "1": "Sunday",
    "2": "Monday",
    "3": "Tuesday",
    "4": "Wednesday",
    "5": "Thursday",
    "6": "Friday",
    "7": "Saturday"}

inp = input("Enter a date ")

while inp.lower() != "done":
    date = datetime.strptime(inp, "%d %m %Y")
    print(week_map.get(str(date.weekday())  ) )
    # and day of the week before 1 month
    one_month_before = date - relativedelta(months=1)
    print(f" date before one month of {date} is {one_month_before}")
    print(f"day of the week before one month of {date} is {week_map.get(str(one_month_before.weekday()))}")
    inp = input("Enter a date ")


