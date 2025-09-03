from collections import namedtuple

from pyspark import SparkConf
from pyspark.sql import *

SurveyRecord = namedtuple("SurveyRecord",["Age","Gender","Country","State"])
if __name__ == "__main__":
    conf = (SparkConf()
            .setMaster("local[2]")
            .setAppName("HelloRDD"))
    spark = (SparkSession
             .builder
             .config(conf=conf)
             .getOrCreate())
    sc = spark.sparkContext
    lineRdd = sc.textFile("../input_data/sample_rdd.csv")
    mappedRdd = lineRdd.map(lambda line: line.replace('"','').split(","))
    selectedRdd = mappedRdd.map(lambda record: SurveyRecord(int(record[1]),record[2],record[3],record[4]))
    filteredRdd = selectedRdd.filter(lambda record: record.Age < 40)
    countryRdd = filteredRdd.map(lambda record: (record.Country, 1))
    countCountryRdd = countryRdd.reduceByKey(lambda v1,v2 : v1+v2)
    for line in countCountryRdd.collect():
        print(line)
