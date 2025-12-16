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

def read_json_from_s3(path):
    """Read JSON files from S3"""
    try:
        df = spark.read.json(path)
        count = df.count()
        log_info(f"✅ Read {count} records from {path}")
        return df
    except Exception as e:
        log_info(f"⚠️ Error reading {path}: {str(e)}")
        return None

def write_parquet_to_s3(df, path, mode='overwrite'):
    """Write DataFrame to S3 as Parquet"""
    try:
        record_count = df.count()
        df.write.mode(mode).parquet(path)
        log_info(f"✅ Wrote {record_count} records to {path}")
        return True
    except Exception as e:
        log_info(f"❌ Error writing to {path}: {str(e)}")
        return False

# ============================================================================
# MAIN ETL PROCESS
# ============================================================================

try:
    log_info("🚀 Starting Glue ETL job")
    log_info(f"Execution Date: {EXECUTION_DATE}")
    log_info(f"Reading from: {RAW_PATH}")
    log_info(f"Writing to: {CURATED_PATH}")
    
    # ========================================================================
    # READ RAW DATA
    # ========================================================================
    
    # Read all raw JSON files from Lambda output
    log_info("Step 1: Reading raw Wistia data...")
    raw_df = read_json_from_s3(f'{RAW_PATH}/**/data.json')
    
    if raw_df is None or raw_df.count() == 0:
        log_info("❌ No raw data found!")
        log_info(f"Expected path: {RAW_PATH}/**/data.json")
        raise Exception("No data to process")
    
    log_info(f"✅ Raw data loaded: {raw_df.count()} files")
    
    # ========================================================================
    # FLATTEN AND TRANSFORM
    # ========================================================================
    
    log_info("Step 2: Transforming data...")
    
    # Extract media info and stats
    dim_media = raw_df.select(
        col('media_id').alias('media_id'),
        col('name').alias('title'),
        col('media_info.hashed_id').alias('hashed_id'),
        col('media_info.description').alias('description'),
        col('media_info.created_at').alias('created_at'),
        col('media_info.duration').alias('duration_seconds'),
        col('media_info.type').alias('media_type'),
        col('media_info.status').alias('status'),
        col('media_info.project_id').alias('project_id'),
        current_timestamp().alias('load_timestamp'),
        lit(EXECUTION_DATE).alias('execution_date')
    ).filter(col('media_id').isNotNull()).dropDuplicates(['media_id'])
    
    log_info(f"✅ DimMedia transformed: {dim_media.count()} records")
    
    # Extract stats (flatten nested structure)
    stats_df = raw_df.select(
        col('media_id'),
        col('name'),
        explode_outer(col('stats')).alias('stat') if 'stats' in raw_df.columns else col('stats')
    )
    
    fact_engagement = raw_df.select(
        col('media_id'),
        col('name').alias('media_name'),
        col('stats.pageLoads').alias('page_loads'),
        col('stats.uniqueVisitors').alias('unique_visitors'),
        col('stats.plays').alias('plays'),
        col('stats.timePlayed').alias('time_played_seconds'),
        col('stats.engagement').alias('engagement_pct'),
        col('stats.cta_clicks').alias('cta_clicks'),
        col('stats.form_submissions').alias('form_submissions'),
        current_timestamp().alias('load_timestamp'),
        lit(EXECUTION_DATE).alias('execution_date')
    ).filter(col('media_id').isNotNull())
    
    log_info(f"✅ FactEngagement transformed: {fact_engagement.count()} records")
    
    # ========================================================================
    # WRITE TO CURATED ZONE
    # ========================================================================
    
    log_info("Step 3: Writing to curated zone...")
    
    # Write dimension table
    write_parquet_to_s3(
        dim_media,
        f'{CURATED_PATH}/dim_media'
    )
    
    # Write fact table
    write_parquet_to_s3(
        fact_engagement,
        f'{CURATED_PATH}/fact_engagement'
    )
    
    log_info("✅ ETL job completed successfully!")
    job.commit()

except Exception as e:
    log_info(f"❌ ERROR in ETL job: {str(e)}")
    import traceback
    log_info(traceback.format_exc())
    job.commit()
    raise e