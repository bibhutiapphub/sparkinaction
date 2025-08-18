from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WordCountApp").getOrCreate()
sc = spark.sparkContext

my_data = ["Hello World", "World Is Beautiful", "Beautiful Mind", "Say Hello"]
data_rdd = sc.parallelize(my_data)
results = data_rdd.flatMap(lambda line: line.split(" ")) \
.map(lambda word: (word, 1)) \
.reduceByKey(lambda a,b : a+b)

for word, count in results.collect():
    print(word, count)

spark.stop()