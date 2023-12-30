import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, desc, to_timestamp, substring, concat, lit


findspark.init()
spark = SparkSession.builder.appName("SparkSQLExample").getOrCreate()


csv_file = "./source/departuredelays.csv"

df = (
    spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(csv_file)
)

# Create a temporary view from the dataframe
df.createOrReplaceTempView("us_delay_flights_tbl")

# Query the temp view using sql

spark.sql(
    """select date, delay, origin, destination
               from us_delay_flights_tbl
              where delay > 120
                and origin = "SFO"
                and destination = "ORD"
              order by delay desc
         """
).show(10)


# Above query using Dataframe API

(
    df.select("date", "delay", "origin", "destination")
    .where(col("delay") > 120)
    .where((col("origin") == "SFO") & (col("destination") == "ORD"))
    .orderBy(desc("delay"))
).show(10)


# To convert the date to proper format and to use API methods

formatted_df = (
    df.withColumn(
        "departure_date",
        concat(
            substring(col("date").cast("string"), 1, 1),
            lit("/"),
            substring(col("date").cast("string"), 2, 2),
            lit(" "),
            substring(col("date").cast("string"), 4, 2),
            lit(":"),
            substring(col("date").cast("string"), 6, 2),
        ),
    )
).show(10)
