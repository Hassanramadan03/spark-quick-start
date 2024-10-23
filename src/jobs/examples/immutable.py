from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()

data = [("James", "Hassan", "Smith", "36636", "M", 60000),
        ("Michael", "Rose", "Ramadan", "40288", "M", 70000),
        ("Robert", "Elsayed", "Williams", "42114", "Selim", 400000),
        ("Maria", "Anne", "Jones", "39192", "F", 500000),
        ("Jen", "Mary", "Brown", "Ali", "F", 10)]
columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]
df = spark.createDataFrame(data=data, schema=columns)
df.show( )


filtered_df = df.filter(df.salary < 50000)
filtered_df.show()