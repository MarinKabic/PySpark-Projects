from pyspark.sql import SparkSession
from pyspark.sql.functions import substring, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Creating a Spark session
spark = SparkSession.builder.appName("CarYearAnalysis").getOrCreate()

# Defining the schema for the dataset
schema = StructType([
    StructField("ID", IntegerType(), True),
    StructField("Car Name", StringType(), True),
    StructField("Manual/Motor", StringType(), True),
    StructField("MPG", IntegerType(), True),
    StructField("Annual Fuel Cost", StringType(), True)
])

# Loading the data from the CSV file
data = spark.read.option("header", "true").schema(schema).csv("cars.csv")

# Extracting the car year from the "Car Name" column (first 4 characters)
data = data.withColumn("CarYear", substring(col("Car Name"), 1, 4))

# Grouping by the car year and counting the occurrences
car_year_counts = data.groupBy("CarYear").count()

# Finding the most common car year
most_common_car_year = car_year_counts.orderBy("count", ascending=False).first()

# Showing the result
print("Most common car year:", most_common_car_year["CarYear"])
print("Count:", most_common_car_year["count"])

# Stop the Spark session
spark.stop()