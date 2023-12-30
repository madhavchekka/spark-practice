import findspark
from pyspark.sql import SparkSession

event_log_dir = "/Users/madhavchekka/spark_logs"

findspark.init()

spark = SparkSession.builder.appName("example").getOrCreate()

# conf = spark.sparkContext.getConf()
# for (key, value) in conf.getAll():
#    print(f"{key}-{value}\n")

strings = spark.read.text("source/README.md")
filtered = strings.filter(strings.value.contains("Spark"))
print(filtered.count())
spark.stop()
