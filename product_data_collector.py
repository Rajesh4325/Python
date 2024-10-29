from pyspark.sql import SparkSession

from iphone_utilities import create_hive_tbl, convert_csv_to_parquet

if __name__ == '__main__':
    spark: SparkSession = SparkSession.builder.master("local[1]").appName(
        "product_data_collector_api").enableHiveSupport().getOrCreate()
    spark.conf.set("hive.exec.dynamic.partition", "true")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    product_file_path = "file:///home/takeo/iphone_product.txt"
    parquet_dir = "file:///home/takeo/parquet/"
    convert_csv_to_parquet(spark, product_file_path, parquet_dir)
    df_parquet = spark.read.options(header=True, inferSchema=True).parquet(parquet_dir)
    output_hive_tbl = "xyz.iphone_product"
    create_hive_tbl(df_parquet, output_hive_tbl)
    spark.stop()