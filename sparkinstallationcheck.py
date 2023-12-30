import findspark
import os
import yaml
from pyspark.sql import SparkSession


def load_config(config_file="config.yaml"):
    with open(config_file, "r") as stream:
        try:
            config = yaml.safe_load(stream)
            return config
        except yaml.YAMLError as exc:
            print(f"Error parsing YAML: {exc}")
            return None


def main():
    # Load the configuration from the YAML file
    config = load_config()

    if config:
        # Access configuration parameters
        os.environ["SPARK_HOME"] = config["spark"]["SPARK_HOME"]
        os.environ["PYSPARK_PYTHON"] = config["spark"]["PYSPARK_PYTHON"]
        os.environ["JAVA_HOME"] = config["spark"]["JAVA_HOME"]
        os.environ["PYSPARK_DRIVER_PYTHON"] = config["spark"]["PYSPARK_DRIVER_PYTHON"]

        # Use the configuration parameters in your application
        print(os.environ["SPARK_HOME"])
        print(os.environ["PYSPARK_PYTHON"])
        print(os.environ["JAVA_HOME"])
        print(os.environ["PYSPARK_DRIVER_PYTHON"])


def start_spark():
    findspark.init()
    spark = SparkSession.builder.appName("example").getOrCreate()
    df = spark.sql("SELECT 'Hello, world!' as message")
    df.show()


if __name__ == "__main__":
    main()
    start_spark()
