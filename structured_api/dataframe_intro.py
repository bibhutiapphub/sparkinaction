from utils.spark_utils import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = get_spark_session()
    file_path = "../input_data/sample.csv"
    survey_raw_df = load_survey_df(spark, file_path)

    filtered_df = (survey_raw_df.where("Age < 40")
                   .select("Age", "Gender", "Country", "State"))
    result_df = filtered_df.groupBy("Country").agg(count("Country"))
    # survey_partitioned_df = survey_raw_df.repartition(2)
    # count_country_df = get_count_by_country_df(survey_raw_df)
    print(result_df.collect())
    input("Please Enter to Abort")
