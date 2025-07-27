from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import timedelta

# Loading the NYC Taxi data into a DataFrame from the external table in Databricks
df = spark.table("yellow_tripdata_2020_01")
df.show(5)           # Checking the first few rows to make sure it loaded correctly
df.printSchema()     # Printing the schema so I know what columns are available

# Query 1: Creating a new column called "Revenue" that adds up all fare-related columns
# I include fare_amount, extra, mta_tax, improvement_surcharge, tip_amount, tolls_amount, and total_amount
df = df.withColumn(
    "Revenue",
    F.col("fare_amount") + F.col("extra") + F.col("mta_tax") +
    F.col("improvement_surcharge") + F.col("tip_amount") +
    F.col("tolls_amount") + F.col("total_amount")
)
df.select("Revenue").show(20)  # Displaying the Revenue column to check the calculation

# Query 2: Counting the total number of passengers for each pickup area in NYC (PULocationID)
passengers_by_area = df.groupBy("PULocationID").agg(
    F.sum("passenger_count").alias("total_passengers")
).orderBy(F.desc("total_passengers"))
passengers_by_area.show()      # Showing which areas have the most passengers

# Query 3: Calculating average fare and total earnings for each vendor (VendorID)
df.groupBy("VendorID").agg(
    F.avg("fare_amount").alias("avg_fare"),
    F.avg("total_amount").alias("avg_total_earning")
).show()                       # This shows how vendors compare in terms of earnings

# Query 4: For each payment mode, I want to see a moving count of how many payments have been made
window_spec = Window.partitionBy("payment_type").orderBy("tpep_pickup_datetime").rowsBetween(-99, 0)
df = df.withColumn("moving_count", F.count("payment_type").over(window_spec))
df.select("payment_type", "tpep_pickup_datetime", "moving_count").show(10)  # Checking the first few moving counts

# Query 5: On a specific date, find the two vendors who made the most money
# Also show how many passengers they had and the total distance their cabs drove
date_str = "2020-01-15"  # If needed, I can change this date
df_date = df.filter(F.to_date("tpep_pickup_datetime") == date_str)

top_vendors = df_date.groupBy("VendorID").agg(
    F.sum("total_amount").alias("total_earning"),
    F.sum("passenger_count").alias("total_passengers"),
    F.sum("trip_distance").alias("total_distance")
).orderBy(F.desc("total_earning")).limit(2)
top_vendors.show()               # This shows the two best vendors for the day

# Query 6: Finding the route (pickup and dropoff location pair) with the most passengers
df.groupBy("PULocationID", "DOLocationID").agg(
    F.sum("passenger_count").alias("total_passengers")
).orderBy(F.desc("total_passengers")).show(1)   # Showing the busiest route

# Query 7: For the last 10 seconds in the data, which pickup locations had the most passengers?
latest_time = df.agg(F.max("tpep_pickup_datetime")).collect()[0][0]
window_start = latest_time - timedelta(seconds=10)

df_recent = df.filter(
    (F.col("tpep_pickup_datetime") >= window_start) & (F.col("tpep_pickup_datetime") <= latest_time)
)
df_recent.groupBy("PULocationID").agg(
    F.sum("passenger_count").alias("total_passengers")
).orderBy(F.desc("total_passengers")).show()    # This gives me the top pickup locations in the last 10 seconds

# Assignment complete!