from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# Initialize Spark session
spark = SparkSession.builder \
    .appName("AvgSalaryCalculator") \
    .getOrCreate()

# Define the input file path
input_file = "/home/datasrc/bigDataTask/retailstore_large.csv"

# Read CSV file into a DataFrame
df = spark.read.csv(input_file, header=True, inferSchema=True)

# Compute the average salary (assuming 'salary' column exists)
avg_salary = df.select(avg("salary").alias("average_salary")).collect()[0]["average_salary"]

# Print result
print(f"Average Salary: {avg_salary}")

# Stop Spark session
spark.stop()

