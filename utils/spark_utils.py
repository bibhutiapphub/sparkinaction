from pyspark import SparkConf
import configparser

from pyspark.sql import SparkSession

spark_conf = SparkConf()


def get_spark_config():
    config = configparser.ConfigParser()
    config.read("../conf/spark_config.conf")

    for (key, value) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, value)

    return spark_conf


def get_spark_session():
    spark_conf = get_spark_config()
    spark = (SparkSession
             .builder
             .config(conf=spark_conf)
             .getOrCreate())
    return spark


def load_survey_df(spark, file_path):
    survey_df = (spark
                 .read
                 .option("header", "true")
                 .option("inferSchema", "true")
                 .csv(file_path))
    return survey_df

def get_count_by_country_df(survey_partitioned_df):
    count_by_country_df = (survey_partitioned_df
                           .where("Age < 40")
                           .select("Age","Gender","Country","State")
                           .groupBy("Country")
                           .count())
    return count_by_country_df
