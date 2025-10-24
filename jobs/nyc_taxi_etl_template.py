#!/usr/bin/env python3
"""
NYC Yellow Taxi ETL (Starter Template with TODOs)

Implement:
- Read monthly Parquet files and the taxi zone lookup CSV
- Clean and transform trip data (types, invalid values, derived columns)
- Join to enrich trips with zone names
- Aggregations:
  1) Hourly pickups per zone
  2) Top 10 zones by daily trip counts
  3) Weekly revenue per vendor
- Write curated Parquet partitioned by pickup_date
- (Optional) Write aggregations to Snowflake (key-pair auth supported via SF_PRIVATE_KEY_B64)
"""
import argparse
import os
import base64
from typing import List, Dict
from pyspark.sql import SparkSession, DataFrame, Window, functions as F, types as T
import os
import time
from dotenv import load_dotenv
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from pyspark import SparkConf
import re

def build_spark(app_name: str, shuffle_partitions: int = 200) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .getOrCreate()
    )
    return spark

def read_trips(spark: SparkSession, input_paths: List[str]) -> DataFrame:
    # TODO: Read multiple Parquet files, e.g., spark.read.parquet(*input_paths)
    input_paths = [os.path.abspath(a_path) for a_path in input_paths]
    print(*input_paths)
    parq_df = spark.read.parquet(*input_paths)

    return parq_df

def read_zones(spark: SparkSession, zone_csv: str) -> DataFrame:
    # TODO: Read CSV with header=True, inferSchema=True
    csv_df = spark.read.options(header=True, inferSchema=True).csv(zone_csv)
    
    return csv_df

def clean_and_transform(df: DataFrame) -> DataFrame:
    """
    Required:
    - Cast fields to correct types (timestamps, doubles/integers)
    - Filter invalid trips:
      * passenger_count <= 0
      * trip_distance <= 0
      * fare_amount < 0
    - Derive:
      * pickup_date (date)
      * pickup_hour (0-23)
      * week_of_year (1-53)
      * vendor_name (1->'Creative', 2->'VeriFone', else id string or 'Unknown')
    """

    #helpful spark functions for converting to date/hour/week: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.weekofyear.html

    df_clean = (
        df.filter(
            (F.col("passenger_count") > 0) &
            (F.col("trip_distance") > 0) &
            (F.col("fare_amount") >= 0) 
        ).withColumn("tpep_pickup_datetime", F.col("tpep_pickup_datetime").cast(T.TimestampType()))
          .withColumn("tpep_dropoff_datetime", F.col("tpep_dropoff_datetime").cast(T.TimestampType()))
          .withColumn("passenger_count", F.col("passenger_count").cast(T.IntegerType()))
          .withColumn("trip_distance", F.col("trip_distance").cast(T.DoubleType()))
          .withColumn("fare_amount", F.col("fare_amount").cast(T.DoubleType()))
          .withColumn("pickup_date", F.to_date(F.col("tpep_pickup_datetime")))
          .withColumn("pickup_hour", F.hour(F.col("tpep_pickup_datetime")))
          .withColumn("week_of_year", F.weekofyear(F.col("tpep_pickup_datetime")))
          .withColumn("vendor_name", F.when(F.col("VendorID")==1, F.lit("Creative")).when(F.col("VendorID")==2, F.lit("VeriFone")).otherwise(F.lit("Unknown")))
    )

    return df_clean

def join_zones(df: DataFrame, zones: DataFrame, use_sort_merge: bool, spark: SparkSession) -> DataFrame:
    """
    Join PULocationID and DOLocationID to zone names.
    - zones: LocationID, Borough, Zone
    - Consider broadcasting zones (small table)
    """
    
    time_start = time.perf_counter()
    zones_select = zones.select("LocationID", "Borough", "Zone")

    #force spark to use sort merge by setting autoBroadcast threshold to 0 bytes
    if use_sort_merge:
        print("using sort merge join")
        #https://stackoverflow.com/questions/48145514/how-to-hint-for-sort-merge-join-or-shuffled-hash-join-and-skip-broadcast-hash-j
        spark.conf.set("spark.sql.join.autoBroadcastJoinThreshold", -1)
        spark.conf.set("spark.sql.join.preferSortMergeJoin", True)
        join_table_right = zones_select
        join_hint = "MERGE"
        print(f"sort merge join settings: {spark.conf.get("spark.sql.join.autoBroadcastJoinThreshold")} | {spark.conf.get("spark.sql.join.preferSortMergeJoin")}")

    else:
        print("using broadcast join")
        join_table_right = F.broadcast(zones_select)
        join_hint = "" #should be broadcast by default

    
    #using hints for joins: https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-hints.html
    #pu join
    pu_joined_df = (
        df.hint(join_hint).join(join_table_right.hint(join_hint), df.PULocationID == zones_select.LocationID, "left")
        .drop("LocationID").withColumnRenamed("Zone", "PU_Zone").withColumnRenamed("Borough", "PU_Borough")
    )
    #do join
    pu_do_joined_df = (
        pu_joined_df.hint(join_hint).join(join_table_right.hint(join_hint), df.DOLocationID == zones_select.LocationID, "left")
        .drop("LocationID").withColumnRenamed("Zone", "DO_Zone").withColumnRenamed("Borough", "DO_Borough")
    )

    time_end = time.perf_counter()
    print(f"join_zones time elapsed: {time_end-time_start:.4f}")
    #print(pu_do_joined_df.select("PULocationID","DOLocationID","PU_Zone", "DO_Zone").show(10))
    print(f"Join Plan:")
    pu_do_joined_df.explain()
    return pu_do_joined_df


def agg_hourly_pickups(df_enriched: DataFrame) -> DataFrame:
    # TODO: group by pickup_date, pickup_hour, PU_Zone (or PU_Borough) and count
    return df_enriched.groupBy("pickup_date", "pickup_hour", "PU_Zone").count()

def agg_top10_zones_daily(df_enriched: DataFrame) -> DataFrame:
    # TODO: daily top 10 zones by trips with rank
    daily_counts = (
        df_enriched.groupBy("pickup_date", "PU_Zone").count().withColumnRenamed("count", "PU_Zone_Count")
    )

    window_spec = Window.partitionBy("pickup_date").orderBy(F.desc("PU_Zone_Count"))
    return daily_counts.withColumn("rank", F.row_number().over(window_spec)).filter(F.col("rank") <= 10)

def agg_weekly_revenue_per_vendor(df_enriched: DataFrame) -> DataFrame:
    # TODO: sum total_amount by vendor_name and week_of_year
    return df_enriched.groupBy("vendor_name", "week_of_year").agg(F.sum("total_amount"))


def write_curated(df_enriched: DataFrame, out_path: str):
    # TODO: write partitioned by pickup_date as parquet (mode overwrite or append)
    #https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.partitionBy.html
    df_enriched.write.partitionBy("pickup_date").mode("overwrite").format("parquet").save(out_path)

def write_aggregates_local(dfs: Dict[str, DataFrame], out_oot: str):
    # TODO: write each df as parquet under out_root/<name>
    #option("compression", "snappy")
    for df_name, df in dfs.items():
        save_path = os.path.join(out_oot, df_name)
        df.write.mode("overwrite").format("parquet").save(save_path)

def _decode_pem_from_b64(b64_str: str) -> str:
    if not b64_str:
        return ""
    return base64.b64decode(b64_str).decode("utf-8")

#for loading pem and connecting to snowflake: https://community.snowflake.com/s/article/How-to-connect-snowflake-with-Spark-connector-using-Public-Private-Key
def _load_pem(pem_path: str) -> str:

    with open(pem_path, "rb") as key_file:
        p_key = serialization.load_pem_private_key(
        key_file.read(),
        password=None,
        backend=default_backend()
        )
    
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    
    pkb = pkb.decode("UTF-8")
    # pkb = re.sub("-*(BEGIN|END) PRIVATE KEY-*\n","",pkb).replace("\n","") #gives missing password error if removing the begin end private key lines

    return pkb



def write_to_snowflake(dfs: Dict[str, DataFrame], sf_opts: Dict[str, str], mode: str = "overwrite"):
    """
    Requires Spark Snowflake connector on classpath.

    For key-pair auth, provide:
      - sfURL, sfUser, sfDatabase, sfSchema, sfWarehouse
      - pem_private_key (decoded PEM string) and optional pem_private_key_passphrase

    Hints (TODO for students):
    - Construct writer = df.write.format("snowflake").options(**opts).option("dbtable", "<TABLE>")
    - Call writer.mode(mode).save()
    """
    # TODO: Implement Snowflake writes using sf_opts.
    for _, df in dfs.items():
        writer = df.write.format("snowflake").options(**sf_opts).option("dbtable", "HW4TABLE")
        writer.mode(mode).save()

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input_paths", nargs="+", required=False, default=["./data/raw/yellow_tripdata_2023-04.parquet", "./data/raw/yellow_tripdata_2023-03.parquet", "./data/raw/yellow_tripdata_2023-02.parquet"])
    p.add_argument("--zone_csv", required=False, default="./data/raw/taxi_zone_lookup.csv")
    p.add_argument("--curated_out", required=False)
    p.add_argument("--aggregates_out", required=False)
    p.add_argument("--shuffle_partitions", type=int, default=200)
    # Snowflake options
    p.add_argument("--write_snowflake", type=str, default="false")
    p.add_argument("--sfURL", default=os.getenv("SF_URL", ""))
    p.add_argument("--sfUser", default=os.getenv("SF_USER", ""))
    p.add_argument("--sfPassword", default=os.getenv("SF_PASSWORD", ""))  # optional fallback
    p.add_argument("--sfDatabase", default=os.getenv("SF_DATABASE", ""))
    p.add_argument("--sfSchema", default=os.getenv("SF_SCHEMA", ""))
    p.add_argument("--sfWarehouse", default=os.getenv("SF_WAREHOUSE", ""))
    p.add_argument("--sfAuthenticator", default=os.getenv("SF_AUTHENTICATOR", "snowflake"))
    p.add_argument("--sfPrivateKeyB64", default=os.getenv("SF_PRIVATE_KEY_B64", ""))
    p.add_argument("--sfPrivateKeyPassphrase", default=os.getenv("SF_PRIVATE_KEY_PASSPHRASE", ""))
    p.add_argument("--sfPrivateKeyPath", type=str, default=os.getenv("SF_PRIVATE_KEY_PATH", "")) #providing direct path to p8 file for snowflake auth
    p.add_argument("--forceSortMerge", action="store_true", help="force spark to use sort merge join.")
    return p.parse_args()

def main():
    load_dotenv()
    args = parse_args()
    spark = build_spark("NYC Taxi ETL (Student)", shuffle_partitions=args.shuffle_partitions)
    print(args.input_paths)
    trips = read_trips(spark, args.input_paths)
    zones = read_zones(spark, args.zone_csv)
    print(trips.show(10))
    print(zones.show(10))

    #measure time for join and aggregate fuctnions
    start_time = time.perf_counter()

    clean = clean_and_transform(trips)
    print(clean.show(10))
    print(clean.select("PULocationID", "DOLocationID").show(10))
    enriched = join_zones(clean, zones, args.forceSortMerge, spark)

    # # Aggregations
    hourly = agg_hourly_pickups(enriched)
    top10 = agg_top10_zones_daily(enriched)
    weekly_rev = agg_weekly_revenue_per_vendor(enriched)

    end_time = time.perf_counter()
    print(hourly.show(10))
    print(top10.show(20))
    print(weekly_rev.show(10))
    print(enriched.show(10))
    print(f"Time elapsed for clean, join, and aggregates: {end_time-start_time:.4f} with {args.shuffle_partitions} partitions. Used sort merge join: {args.forceSortMerge}")

    # Outputs
    # write_curated(enriched, args.curated_out)
    # write_aggregates_local(
    #     {"hourly_pickups": hourly, "top10_zones_daily": top10, "weekly_revenue_vendor": weekly_rev},
    #     args.aggregates_out,
    # )


    if args.write_snowflake.lower() == "true":
        sf_opts = {
            "sfURL": args.sfURL,
            "sfUser": args.sfUser,
            "sfDatabase": args.sfDatabase,
            "sfSchema": args.sfSchema,
            "sfWarehouse": args.sfWarehouse,
            "sfAuthenticator": args.sfAuthenticator,
        }

        # snowflake auth by loading in pem file directly https://community.snowflake.com/s/article/How-to-connect-snowflake-with-Spark-connector-using-Public-Private-Key
        # Prefer key-pair if provided
        #pem = _decode_pem_from_b64(args.sfPrivateKeyB64)
        # pem = _load_pem(args.sfPrivateKeyPath)
        # if pem:

        #     sf_opts["pem_private_key"] = pem

        #     if args.sfPrivateKeyPassphrase:
        #         sf_opts["pem_private_key_passphrase"] = args.sfPrivateKeyPassphrase
        # elif args.sfPassword:
        #     # Fallback to username/password if allowed by your account policy
        #     sf_opts["sfPassword"] = args.sfPassword

        # write_to_snowflake(
        #     {"HOURLY_PICKUPS": hourly, "TOP10_ZONES_DAILY": top10, "WEEKLY_REVENUE_VENDOR": weekly_rev},
        #     sf_opts,
        #     mode="overwrite",
        # )

    spark.stop()

if __name__ == "__main__":
    main()
