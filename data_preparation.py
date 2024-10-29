from pyspark.sql import SparkSession
from iphone_utilities import  create_iphone_output

if __name__ == '__main__':
    spark: SparkSession = SparkSession.builder.master("local[1]").appName(
        "data_preparation_api").enableHiveSupport().getOrCreate()
    output_hive_tbl = create_iphone_output(spark)
    spark.stop()