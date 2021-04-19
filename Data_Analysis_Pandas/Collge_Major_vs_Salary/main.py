import pandas as pd
from tabulate import tabulate

pd.set_option('display.width', 400)
pd.set_option('display.max_columns', 12)

data = pd.read_csv("salaries_by_college_major.csv")

print(data.head(5))

