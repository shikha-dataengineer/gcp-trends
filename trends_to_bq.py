from pytrends.request import TrendReq
from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession.builder \
    .appName("GoogleTrendsToCSV") \
    .getOrCreate()

# ----------------------------
# Initialize Pytrends
#tz = 360 Timezone offset in minutes
# ----------------------------
# Below line 14 creates a Pytrends session object you can use to fetch data.
pytrends = TrendReq(hl='en-US', tz=360)
keywords = ["AI", "Electric Cars", "Climate Change"]

all_dfs = []

# ----------------------------
# Fetch Google Trends
# timeframe='today 12-m' -- last 12 months data
# ----------------------------
for kw in keywords:
    pytrends.build_payload([kw], timeframe='today 12-m', geo='')
    df = pytrends.interest_over_time().reset_index()
    df['keyword'] = kw
    all_dfs.append(df)

# Combine all data
combined_df = pd.concat(all_dfs, ignore_index=True)
combined_df.drop(columns=['isPartial'], inplace=True, errors='ignore')

# ----------------------------
# Convert Pandas DF to Spark DF
# ----------------------------
spark_df = spark.createDataFrame(combined_df)

# ----------------------------
# 5. Write locally as CSV
# ----------------------------
spark_df.coalesce(1).write.csv("output/trends_output.csv", header=True, mode="overwrite")

print("✅ Live Google Trends data fetched and saved locally!")

