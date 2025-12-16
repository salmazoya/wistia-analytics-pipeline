"""
Wistia Analytics Glue ETL Job
Transforms raw JSON data from S3 into curated Parquet format
Using PySpark and AWS Glue with explicit schema
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
from datetime import datetime

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'execution_date'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configuration
S3_BUCKET = 'wistia-analytics'
EXECUTION_DATE = args.get('execution_date', datetime.now().strftime('%Y-%m-%d'))
RAW_BASE_PATH = f's3://{S3_BUCKET}/raw/wistia/execution_date={EXECUTION_DATE}'
CURATED_PATH = f's3://{S3_BUCKET}/curated/wistia/execution_date={EXECUTION_DATE}'

logger = glueContext.get_logger()

def log_info(message):
    """Log info message"""
    logger.info(f"[GLUE ETL] {message}")

# ============================================================================
# SCHEMA DEFINITION
# ============================================================================

# Define explicit schema to handle nested JSON properly
raw_schema = StructType([
    StructField("media_id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("media_info", StructType([
        StructField("id", LongType(), True),
        StructField("hashed_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("type", StringType(), True),
        StructField("status", StringType(), True),
        StructField("archived", BooleanType(), True),
        StructField("duration", DoubleType(), True),
        StructField("created", StringType(), True),
        StructField("updated", StringType(), True),
        StructField("project", StructType([
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
            StructField("hashed_id", StringType(), True),
        ]), True),
    ]), True),
    StructField("stats", StructType([
        StructField("load_count", LongType(), True),
        StructField("play_count", LongType(), True),
        StructField("play_rate", DoubleType(), True),
        StructField("hours_watched", DoubleType(), True),
        StructField("engagement", DoubleType(), True),
        StructField("visitors", LongType(), True),
    ]), True),
    StructField("ingestion_timestamp", StringType(), True),
])

# ============================================================================
# MAIN ETL PROCESS
# ============================================================================

try:
    log_info("🚀 Starting Glue ETL job")
    log_info(f"Execution Date: {EXECUTION_DATE}")
    log_info(f"Reading from: {RAW_BASE_PATH}")
    
    # ========================================================================
    # READ RAW DATA with explicit schema
    # ========================================================================
    
    log_info("Step 1: Reading raw Wistia data from S3...")
    
    try:
        # Read with explicit schema
        raw_path = f'{RAW_BASE_PATH}/**/data.json'
        log_info(f"Reading from path: {raw_path}")
        
        raw_df = spark.read \
            .schema(raw_schema) \
            .json(raw_path)
        
        raw_count = raw_df.count()
        
        if raw_count == 0:
            log_info(f"❌ No data found at {raw_path}")
            raise Exception("No data to process")
        
        log_info(f"✅ Raw data loaded: {raw_count} records")
        raw_df.printSchema()
        
    except Exception as e:
        log_info(f"❌ Error reading from {raw_path}: {str(e)}")
        raise
    
    # ========================================================================
    # TRANSFORM: CREATE DIMENSION TABLES
    # ========================================================================
    
    log_info("Step 2: Transforming data...")
    
    # ---- DIM_MEDIA ----
    log_info("Creating DIM_MEDIA...")
    
    try:
        dim_media = raw_df.select(
            col('media_id').alias('media_id'),
            col('name').alias('title'),
            col('media_info.hashed_id').alias('hashed_id'),
            col('media_info.description').alias('description'),
            col('media_info.created').alias('created_at'),
            col('media_info.updated').alias('updated_at'),
            col('media_info.duration').alias('duration_seconds'),
            col('media_info.type').alias('media_type'),
            col('media_info.status').alias('status'),
            col('media_info.archived').alias('archived'),
            col('media_info.project.id').alias('project_id'),
            col('media_info.project.name').alias('project_name'),
            current_timestamp().alias('load_timestamp'),
            lit(EXECUTION_DATE).alias('execution_date')
        ).dropDuplicates(['media_id'])
        
        dim_media_count = dim_media.count()
        log_info(f"✅ DIM_MEDIA: {dim_media_count} records")
        
    except Exception as e:
        log_info(f"❌ Error creating DIM_MEDIA: {str(e)}")
        raise
    
    # ========================================================================
    # TRANSFORM: CREATE FACT TABLES
    # ========================================================================
    
    # ---- FACT_ENGAGEMENT ----
    log_info("Creating FACT_ENGAGEMENT...")
    
    try:
        fact_engagement = raw_df.select(
            col('media_id').alias('media_id'),
            col('name').alias('media_name'),
            col('stats.load_count').alias('page_loads'),
            col('stats.play_count').alias('plays'),
            col('stats.play_rate').alias('play_rate'),
            col('stats.hours_watched').alias('hours_watched'),
            col('stats.engagement').alias('engagement_pct'),
            col('stats.visitors').alias('unique_visitors'),
            current_timestamp().alias('load_timestamp'),
            lit(EXECUTION_DATE).alias('execution_date')
        )
        
        fact_engagement_count = fact_engagement.count()
        log_info(f"✅ FACT_ENGAGEMENT: {fact_engagement_count} records")
        
    except Exception as e:
        log_info(f"❌ Error creating FACT_ENGAGEMENT: {str(e)}")
        raise
    
    # ========================================================================
    # WRITE TO CURATED ZONE
    # ========================================================================
    
    log_info("Step 3: Writing to curated zone...")
    
    # Write DIM_MEDIA
    try:
        dim_media.coalesce(1).write.mode('overwrite').parquet(f'{CURATED_PATH}/dim_media')
        log_info(f"✅ Wrote DIM_MEDIA to {CURATED_PATH}/dim_media")
    except Exception as e:
        log_info(f"❌ Error writing DIM_MEDIA: {str(e)}")
        raise
    
    # Write FACT_ENGAGEMENT
    try:
        fact_engagement.coalesce(1).write.mode('overwrite').parquet(f'{CURATED_PATH}/fact_engagement')
        log_info(f"✅ Wrote FACT_ENGAGEMENT to {CURATED_PATH}/fact_engagement")
    except Exception as e:
        log_info(f"❌ Error writing FACT_ENGAGEMENT: {str(e)}")
        raise
    
    # ========================================================================
    # SUMMARY
    # ========================================================================
    
    log_info("")
    log_info("=" * 50)
    log_info("✅ ETL JOB COMPLETED SUCCESSFULLY")
    log_info("=" * 50)
    log_info(f"DIM_MEDIA records: {dim_media_count}")
    log_info(f"FACT_ENGAGEMENT records: {fact_engagement_count}")
    log_info(f"Output location: {CURATED_PATH}")
    log_info("=" * 50)
    
    job.commit()

except Exception as e:
    log_info("")
    log_info("❌ ERROR IN ETL JOB")
    log_info(str(e))
    import traceback
    log_info(traceback.format_exc())
    job.commit()
    raise e