from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta


""" This is very good exmaple of datetime in python 
each code is important here """

known_format_list = ["%Y/%m/%d", "%d-%m-%Y", "%Y%m%d"]

# A function which can handle any given string format to convert to date

def convert_str_to_date(str):
    for frmt in known_format_list:
        try:
            date_frm = datetime.strptime(str , frmt).date()
            print(date_frm)
            return date_frm

        except Exception:
            print(str)
            continue

date = convert_str_to_date("20150504")
print(type(date))


def convert_date_sting(d : datetime):
    str = d.strftime("%Y-%m-%d")
    print(str)

str = convert_date_sting(date)
print(type(str))

## calculate month diff between 2 dates

def date_calculation_test(start, end):
    start_date = datetime.strptime(start , "%Y-%m-%d %H:%M:%S")
    end_date = datetime.strptime(end , "%Y-%m-%d %H:%M:%S")
    #days_diff = end_date - start_date
    #end_date.month
    #print(days_diff)
    diff = relativedelta(start_date, end_date)
    print(diff)
    # add days / months / hours anything this way , remember NOTE
    add_2_months = end_date + relativedelta(months=2)
    print(add_2_months.month)
    add_2_hour = end_date + relativedelta(hours=2)
    print(add_2_hour)


def calculate_months_diff(start, end):
    start_date = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
    end_date = datetime.strptime(end, "%Y-%m-%d %H:%M:%S")
    r = relativedelta(end_date , start_date)
    months_diff = r.months + (12 * r.years)
    days_diff  = r.days
    print(days_diff)

    if days_diff > 0:
        months_diff += 1
    print(months_diff)


calculate_months_diff("2021-04-28 10:10:00" ,"2021-05-28 11:10:12")


## Challenge 1, print alll the dates

def all_dates_between(start, end):

    start_date = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
    end_date = datetime.strptime(end, "%Y-%m-%d %H:%M:%S")
    print(end_date)
    print(start_date)
    print(start_date < end_date)

    print(start_date + relativedelta(day=1))
    while start_date < end_date:
        print("aaaaa" ,start_date < end_date)
        next_date = start_date + relativedelta(days=1)
        print(f"dates {next_date}")
        start_date = next_date



all_dates_between("2021-04-28 10:10:00" ,"2021-05-28 11:10:12")

