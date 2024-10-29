def create_dataframe_from_file(spark, input_file, delimiter):
    df = spark.read.format("csv").options(header=True, inferSchema=True, delimiter=delimiter).load(input_file)
    return df

def create_hive_tbl(input_df, output_tbl):
    input_df.write.mode("overwrite").saveAsTable(output_tbl)

def convert_csv_to_parquet(spark, input_file, parquet_output_dir):
    df_file = create_dataframe_from_file(spark, input_file,  "|")
    df_file.write.mode("overwrite").parquet(parquet_output_dir)

def create_partitioned_hive_tbl(spark, input_df, output_tbl):
    spark.conf.set("hive.exec.dynamic.partition", "true")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    input_df.write.mode("overwrite").partitionBy("sale_date").format("parquet").saveAsTable(output_tbl)

def create_df_from_parquet_file(spark, input_dir):
    return spark.read.options(header=True, inferSchema=True).parquet(input_dir)

def create_iphone_output(spark):
    spark.conf.set("hive.exec.dynamic.partition", "true")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    df = spark.sql("""
            WITH s8_buyer AS (
                SELECT buyer_id FROM xyz.iphone_sales WHERE product_id IN (SELECT product_id FROM xyz.iphone_product WHERE product_name = 'S8')
            ), iphone_buyer AS (
                SELECT buyer_id FROM xyz.iphone_sales WHERE product_id IN (SELECT product_id FROM xyz.iphone_product WHERE product_name = 'iPhone')
            )
            SELECT buyer_id FROM xyz.iphone_sales WHERE product_id IN (SELECT product_id FROM xyz.iphone_product WHERE product_name = 'S8')
            MINUS
            SELECT s.buyer_id FROM s8_buyer s JOIN iphone_buyer i ON s.buyer_id = i.buyer_id
            """)
    output_hive_tbl = "xyz.iphone_output"
    df.write.mode("overwrite").format("parquet").saveAsTable(output_hive_tbl)




