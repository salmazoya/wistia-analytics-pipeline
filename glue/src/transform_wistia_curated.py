"""
Wistia Analytics Glue ETL Job
Transforms raw JSON data from S3 into curated Parquet format
Using PySpark and AWS Glue
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
from datetime import datetime, timedelta

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
RAW_PATH = f's3://{S3_BUCKET}/raw/wistia/execution_date={EXECUTION_DATE}'
CURATED_PATH = f's3://{S3_BUCKET}/curated/wistia/execution_date={EXECUTION_DATE}'

logger = glueContext.get_logger()

def log_info(message):
    """Log info message"""
    logger.info(f"[GLUE ETL] {message}")

# ============================================================================
# MAIN ETL PROCESS
# ============================================================================

try:
    log_info("🚀 Starting Glue ETL job")
    log_info(f"Execution Date: {EXECUTION_DATE}")
    log_info(f"Reading from: {RAW_PATH}")
    log_info(f"Writing to: {CURATED_PATH}")
    
    # ========================================================================
    # READ RAW DATA - Use wildcard pattern to read all JSON files
    # ========================================================================
    
    log_info("Step 1: Reading raw Wistia data from S3...")
    
    # Read all JSON files with wildcard pattern
    # This reads from: s3://wistia-analytics/raw/wistia/execution_date=2025-12-16/media_id=*/data.json
    raw_path_pattern = f'{RAW_PATH}/**/data.json'
    
    try:
        raw_df = spark.read.json(raw_path_pattern)
        raw_count = raw_df.count()
        
        if raw_count == 0:
            log_info(f"❌ No data found at {raw_path_pattern}")
            raise Exception("No data to process")
        
        log_info(f"✅ Raw data loaded: {raw_count} records")
        
    except Exception as e:
        log_info(f"❌ Error reading from {raw_path_pattern}: {str(e)}")
        raise
    
    # ========================================================================
    # TRANSFORM: CREATE DIMENSION TABLES
    # ========================================================================
    
    log_info("Step 2: Transforming data...")
    
    # ---- DIM_MEDIA ----
    log_info("Creating DIM_MEDIA...")
    
    dim_media = raw_df.select(
        col('media_id').cast('long').alias('media_id'),
        col('name').alias('title'),
        col('media_info.hashed_id').alias('hashed_id'),
        col('media_info.description').alias('description'),
        col('media_info.created').alias('created_at'),
        col('media_info.updated').alias('updated_at'),
        col('media_info.duration').cast('double').alias('duration_seconds'),
        col('media_info.type').alias('media_type'),
        col('media_info.status').alias('status'),
        col('media_info.archived').cast('boolean').alias('archived'),
        col('media_info.project.id').cast('long').alias('project_id'),
        col('media_info.project.name').alias('project_name'),
        current_timestamp().alias('load_timestamp'),
        lit(EXECUTION_DATE).alias('execution_date')
    ).filter(col('media_id').isNotNull()).dropDuplicates(['media_id'])
    
    dim_media_count = dim_media.count()
    log_info(f"✅ DIM_MEDIA: {dim_media_count} records")
    
    # ========================================================================
    # TRANSFORM: CREATE FACT TABLES
    # ========================================================================
    
    # ---- FACT_ENGAGEMENT ----
    log_info("Creating FACT_ENGAGEMENT...")
    
    fact_engagement = raw_df.select(
        col('media_id').cast('long').alias('media_id'),
        col('name').alias('media_name'),
        col('stats.load_count').cast('long').alias('page_loads'),
        col('stats.play_count').cast('long').alias('plays'),
        col('stats.play_rate').cast('double').alias('play_rate'),
        col('stats.hours_watched').cast('double').alias('hours_watched'),
        col('stats.engagement').cast('double').alias('engagement_pct'),
        col('stats.visitors').cast('long').alias('unique_visitors'),
        current_timestamp().alias('load_timestamp'),
        lit(EXECUTION_DATE).alias('execution_date')
    ).filter(col('media_id').isNotNull())
    
    fact_engagement_count = fact_engagement.count()
    log_info(f"✅ FACT_ENGAGEMENT: {fact_engagement_count} records")
    
    # ========================================================================
    # WRITE TO CURATED ZONE
    # ========================================================================
    
    log_info("Step 3: Writing to curated zone...")
    
    # Write DIM_MEDIA
    try:
        dim_media.write.mode('overwrite').parquet(f'{CURATED_PATH}/dim_media')
        log_info(f"✅ Wrote DIM_MEDIA to {CURATED_PATH}/dim_media")
    except Exception as e:
        log_info(f"❌ Error writing DIM_MEDIA: {str(e)}")
        raise
    
    # Write FACT_ENGAGEMENT
    try:
        fact_engagement.write.mode('overwrite').parquet(f'{CURATED_PATH}/fact_engagement')
        log_info(f"✅ Wrote FACT_ENGAGEMENT to {CURATED_PATH}/fact_engagement")
    except Exception as e:
        log_info(f"❌ Error writing FACT_ENGAGEMENT: {str(e)}")
        raise
    
    # ========================================================================
    # SUMMARY
    # ========================================================================
    
    log_info("")
    log_info("========================================")
    log_info("✅ ETL JOB COMPLETED SUCCESSFULLY")
    log_info("========================================")
    log_info(f"DIM_MEDIA records: {dim_media_count}")
    log_info(f"FACT_ENGAGEMENT records: {fact_engagement_count}")
    log_info(f"Output location: {CURATED_PATH}")
    log_info("========================================")
    
    job.commit()

except Exception as e:
    log_info(f"")
    log_info(f"❌ ERROR in ETL job: {str(e)}")
    import traceback
    log_info(traceback.format_exc())
    job.commit()
    raise e