from pyspark.sql import *
from pyspark.sql.functions import *
from twisted.python.compat import StringType


def parse_gender(gender):
    if gender == "M" or gender == "Male":
        return "Male"
    elif gender == "F" or gender == "Female":
        return "Female"
    else:
        return "Unknown"


if __name__ == "__main__":
    spark = (SparkSession.builder
             .master("local[2]")
             .appName("SparkColumnTransformation")
             .getOrCreate())

    survey_df = (spark.read
                 .format("csv")
                 .option("header", "true")
                 .option("inferSchema", "true")
                 .load("../input_data/survey.csv"))

    # This kind of udf registration is for the API level. It will not go to the catalog rather serialized and go to the executors.
    parse_gender_udf = udf(parse_gender)

    print("Before udf registration to catalog")

    # Checking in catalog
    print([f.name for f in spark.catalog.listFunctions() if "parse_gender" in f.name])

    parsed_df = survey_df.withColumn("Gender", parse_gender_udf(col("Gender")))
    parsed_df.show()

    # This kind of udf registration is for the SQL expression
    print("After udf registration to catalog")
    spark.udf.register("parse_gender_udf", parse_gender)
    print([f.name for f in spark.catalog.listFunctions() if "parse_gender" in f.name])

    # Use of the udf as SQL expression
    parsed_df = survey_df.withColumn("Gender", expr("parse_gender_udf(Gender)"))
    parsed_df.show()
