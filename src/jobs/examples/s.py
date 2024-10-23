from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()
sc = spark.sparkContext

# Test Immutable RDDs
numbers = [1, 2, 3, 4, 5]
numbers_rdd = sc.parallelize(numbers)
print(f"Original RDD ID: {numbers_rdd.id()}")
# # Apply a transformation: multiply each number by 2
numbers_rdd = numbers_rdd.map(lambda x: x * 2)
print(f"Transformed RDD ID: {numbers_rdd.id()}")

# # Collect the results to trigger the computation
result = numbers_rdd.collect()
print(f"Transformed RDD result: {result}")