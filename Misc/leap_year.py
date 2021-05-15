

def check_leap_year(year):
    if year%4 ==0:
        if (year%100 == 0) & (year%400 ==0):
            print(f"{year} is a leap year!!")
        elif (year%100 == 0) & (year%400 !=0):
            print(f"{year} is a not leap year!!")
        else:
            print(f"{year} is a leap year!!")
    else:
        print(f"{year} is a not leap year!!")


for i in [2000, 2004, 2008, 2012, 2016, 2020,1900]:
    check_leap_year(i)
