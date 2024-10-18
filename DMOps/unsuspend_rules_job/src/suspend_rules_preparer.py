import click as click
from CMSSpark.spark_utils import get_spark_session
from pyspark.sql.functions import col, lit, lower, when, from_unixtime, to_date, hex as _hex
from pyspark.storagelevel import StorageLevel
from datetime import datetime, timedelta

@click.command()
def main():
    spark = get_spark_session(app_name='cmstools-rucio-suspended-rules')

    # Set the date based on current time, fallback to previous day if before 6 AM
    TODAY = datetime.today().strftime('%Y-%m-%d') if datetime.now().hour >= 6 else (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

    # Define HDFS paths for Rucio data
    HDFS_RUCIO_RULES = f"/project/awg/cms/rucio/{TODAY}/rules/part*.avro"
    HDFS_RUCIO_RSES = f"/project/awg/cms/rucio/{TODAY}/rses/part*.avro"
    HDFS_RUCIO_REPLICAS = f"/project/awg/cms/rucio/{TODAY}/replicas/part*.avro"
    HDFS_RUCIO_LOCKS = f"/project/awg/cms/rucio/{TODAY}/locks/part*.avro"

    # Load suspended rules, filtering for status 'U', convert IDs to lowercase hexadecimal
    df_suspended_rules = spark.read.format('avro').load(HDFS_RUCIO_RULES)\
        .withColumn('RULE_ID', lower(_hex(col('ID'))))\
        .filter(col('STATE') == 'U')\
        .select(['RULE_ID', 'NAME', 'DID_TYPE', 'ERROR']).alias('r')

    # Load RSEs and convert IDs to lowercase hexadecimal
    df_rses = spark.read.format('avro').load(HDFS_RUCIO_RSES)\
        .withColumn('ID', lower(_hex(col('ID'))))\
        .select(['RSE', 'ID'])

    # Load replicas, convert RSE_ID to lowercase hexadecimal, and join with RSEs
    df_replicas = spark.read.format('avro').load(HDFS_RUCIO_REPLICAS)\
            .withColumn('RSE_ID', lower(_hex(col('RSE_ID'))))\
            .withColumnRenamed('STATE','REPLICA_STATE')\
            .select(['RSE_ID', 'NAME', 'REPLICA_STATE'])
    df_replicas = df_replicas.join(df_rses, df_replicas.RSE_ID==df_rses.ID, how='inner')\
                .select(['NAME', 'RSE', 'REPLICA_STATE'])

    # Load locks, filter obsolete ones, convert IDs to lowercase hexadecimal, and join with RSEs and suspended rules
    df_locks = spark.read.format('avro').load(HDFS_RUCIO_LOCKS)\
        .withColumn('RSE_ID', lower(_hex(col('RSE_ID'))))\
        .withColumn('LOCK_RULE_ID', lower(_hex(col('RULE_ID'))))\
        .withColumn('CREATED_AT',to_date(from_unixtime(col('CREATED_AT')/1000,'yyyy-MM-dd'),'yyyy-MM-dd'))\
        .withColumnRenamed('STATE','LOCK_STATE')\
        .withColumnRenamed('NAME','LOCK_NAME')\
        .filter(col('LOCK_STATE')!='O')\
        .select(['LOCK_NAME','CREATED_AT','LOCK_RULE_ID','RSE_ID']).alias("l")
    df_locks = df_locks.join(df_rses, df_locks.RSE_ID==df_rses.ID, how='inner').select(['l.*','RSE']).alias("l")

    df_locks = df_locks.join(df_suspended_rules,df_locks.LOCK_RULE_ID == df_suspended_rules.RULE_ID,how='right')\
        .select(['l.*','RULE_ID','ERROR']).drop('LOCK_RULE_ID')\
        .withColumnRenamed('RULE_ID','LOCK_RULE_ID')\
        .persist(storageLevel=StorageLevel.MEMORY_AND_DISK).alias('l')

    # Map specific error substrings to replacement error codes
    error_mappings_rules = [
        ("HTTP 404", "TRANSFER_FAILED:SOURCE HTTP 404"),
        ("Destination file exists and overwrite is not enabled", "TRANSFER_FAILED:DESTINATION OVERWRITE"),
        ("DESTINATION OVERWRITE", "TRANSFER_FAILED:DESTINATION OVERWRITE"),
        ("CHECKSUM MISMATCH", "CHECKSUM MISMATCH")
    ]

    # Apply error mappings
    for error_substring, replacement in error_mappings_rules:
        df_locks = df_locks.withColumn("ERROR", when(col("ERROR").contains(error_substring), lit(replacement)).otherwise(col("ERROR")))

    # Save locks with no LOCK_NAME as 'ok_suspended_rules.txt'
    df_locks.filter(col('LOCK_NAME').isNull()).select("LOCK_RULE_ID").toPandas().to_csv("ok_suspended_rules.txt", index=False, header=False)

    # Filter locks with "NO_SOURCES" error and join with replicas
    df_no_sources_locks_rep = df_locks.filter(col("ERROR").contains("NO_SOURCES"))\
        .join(df_replicas, df_locks.LOCK_NAME == df_replicas.NAME, "inner")\
        .select(df_locks["*"], col("REPLICA_STATE"))

    # Get distinct LOCK_NAMEs with REPLICA_STATE 'A'
    df_locks_av = df_no_sources_locks_rep.filter(col("REPLICA_STATE") == "A").select("LOCK_NAME").distinct()

    # Get LOCK_NAMEs that don't have REPLICA_STATE 'A'
    df_no_sources_locks = df_no_sources_locks_rep.select("LOCK_NAME").distinct().subtract(df_locks_av)

    # Join to get files without valid replicas and export
    df_no_sources_locks.join(df_no_sources_locks_rep, "LOCK_NAME").select("LOCK_NAME", "RSE", "LOCK_RULE_ID").distinct()\
        .toPandas().to_csv("no_sources_suspended_files.csv", index=False)

    # Further filtering and export for no source error and write exclusions
    df_locks.filter(col('ERROR') == 'RSE excluded; not available for writing.\nDetails: RSE excluded; not available for writing.')\
        .select(["LOCK_RULE_ID", "RSE"]).distinct().toPandas().to_csv("writing_excluded_sites_suspended_rules.csv", index=False)

if __name__ == "__main__":
    main()