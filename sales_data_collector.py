from pyspark.sql import SparkSession

from iphone_utilities import create_dataframe_from_file, create_partitioned_hive_tbl

if __name__ == '__main__':
    spark: SparkSession = SparkSession.builder.master("local[1]").appName(
        "sales_data_collector_api").enableHiveSupport().getOrCreate()

    sales_file_path = "file:///home/takeo/iphone_sales.txt"
    df_sales = create_dataframe_from_file(spark, sales_file_path, "|")
    print(df_sales.show())
    output_hive_tbl = "xyz.iphone_sales"
    create_partitioned_hive_tbl(spark, df_sales, output_hive_tbl)
    spark.stop()
