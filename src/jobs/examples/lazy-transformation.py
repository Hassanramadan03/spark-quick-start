from pyspark.sql import SparkSession

# intiallize sark session
spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()
sc=spark.sparkContext
data = [("James", 61),
        ("Michael", 71),
        ("Robert", 41),
        ("Maria", 51),
        ("Jen", 10)]
columns = ["name", "age","isLess50"]

# create a RDD from data
rdd = sc.parallelize(data)
# apply map transaformation to a tuble if age > 50 to be a boolean flag
mapped_rdd = rdd.map(lambda x: (x[0], x[1], x[1] > 50))
# filter the RDD where age > 50
filtered_rdd = mapped_rdd.filter(lambda x: x[2])
# convert the RDD to DataFrame
df=spark.createDataFrame(filtered_rdd, columns)
# select only name and age columns
final_df = df.select("name", "age")

final_df.show()
results = final_df.collect()
print(results)


# Demo: RDD Text Manipulation
text = ["Hello Spark", "Hello Scala", "Hello World"]
text_rdd = sc.parallelize(text)
print(f"Original Text RDD result: {text_rdd.take(10)}")
# Convert to lowercase
lower_rdd = text_rdd.map(lambda x: x.lower())
print(f"Lowercase Text RDD result: {lower_rdd.take(10)}")

words_rdd = lower_rdd.flatMap(lambda line: line.split(" "))
upper_rdd = words_rdd.map(lambda x: x.upper())
print(f"Words RDD result: {upper_rdd.take(10)}")

spark.stop()