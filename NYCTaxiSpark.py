from pyspark import SparkConf, SparkContext
import pyspark.pandas as ps
import datetime as dt
import glob
import pyarrow.parquet as pq

conf = SparkConf()
#conf.setMaster("spark://10.80.23.119:7077")
conf.setAppName("nyctaxi")
conf.set("spark.executer.memory", "2g")
conf.set("spark.executer.cores", "1")
conf.set("spark.driver.memory", "8g")
conf.set("spark.driver.cores", "5")
spark = SparkContext(conf=conf)

df = ps.read_parquet("/data/nyctaxi/set1/*.parquet")
print(df.columns)
#print(df.info(verbose=True))

#mean = df['fare_amount'].mean()
#print(mean)

#Average ratio of trip cost that is tolls
#Total num of trips per month across all years
#Average price per mile, excluding tolls and mta taxes
#Most popular pickup/dropoff locations (use lat/long but rounded to 3 decimal places)

#print(df.groupby('payment_type')['fare_amount'].mean())

#Total num of trips per month across all years
df = df.dropna()
trip_num = df.groupby(['trip_id']).dt.month
print(trip_num)
