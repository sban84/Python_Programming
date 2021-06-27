from datetime import datetime
s1 = "Sun 10 May 2015 13:54:36 -0700"
d1 = datetime.strptime(s1, '%a %d %b %Y %H:%M:%S %z')

s2 = "Sun 10 May 2015 13:54:36 -0000"
d2 = datetime.strptime(s2, '%a %d %b %Y %H:%M:%S %z')

print(d1-d2) 