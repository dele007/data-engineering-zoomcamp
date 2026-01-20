import sys
import pandas as pd


month = int(sys.argv[1])

df = pd.DataFrame({"day": [1,2,3], "passengers": [30,20,10]})  
df['month'] = month
print(df.head())

df.to_parquet(f'passengers_month={month}.parquet')

print(f'Pipeline module loaded successfully for month = {month}.')