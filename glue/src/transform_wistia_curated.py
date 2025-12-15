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
RAW_PATH = f's3://{S3_BUCKET}/raw'
CURATED_PATH = f's3://{S3_BUCKET}/curated'

logger = glueContext.get_logger()

def log_info(message):
    """Log info message"""
    logger.info(f"[GLUE ETL] {message}")

def read_json_from_s3(path):
    """Read JSON files from S3"""
    try:
        df = spark.read.json(path)
        log_info(f"Read {df.count()} records from {path}")
        return df
    except Exception as e:
        log_info(f"Error reading {path}: {str(e)}")
        return None

def write_parquet_to_s3(df, path, mode='overwrite'):
    """Write DataFrame to S3 as Parquet"""
    try:
        df.write.mode(mode).parquet(path)
        log_info(f"Wrote {df.count()} records to {path}")
        return True
    except Exception as e:
        log_info(f"Error writing to {path}: {str(e)}")
        return False

# ============================================================================
# DIMENSION TABLES
# ============================================================================

def create_dim_media():
    """Create DimMedia dimension table"""
    log_info("Creating DimMedia...")
    
    # Read raw media metadata
    media_path = f'{RAW_PATH}/media_metadata/dt={EXECUTION_DATE}/'
    df = read_json_from_s3(media_path)
    
    if df is None or df.count() == 0:
        log_info("No media metadata found")
        return None
    
    # Transform: Select and rename columns
    dim_media = df.select(
        col('id').alias('media_id'),
        col('hashed_id').alias('media_hashed_id'),
        col('name').alias('title'),
        col('description').alias('description'),
        col('created_at').alias('created_at'),
        col('duration').alias('duration_seconds'),
        col('type').alias('channel'),
        col('project_id').alias('project_id'),
        col('status').alias('status'),
        current_timestamp().alias('load_timestamp')
    )
    
    log_info(f"DimMedia created with {dim_media.count()} records")
    return dim_media

def create_dim_date():
    """Create DimDate dimension table"""
    log_info("Creating DimDate...")
    
    # Generate date range (past 2 years)
    start_date = (datetime.strptime(EXECUTION_DATE, '%Y-%m-%d') - timedelta(days=730))
    end_date = datetime.strptime(EXECUTION_DATE, '%Y-%m-%d')
    
    date_range = spark.sparkContext.parallelize(
        [(start_date + timedelta(days=i)).strftime('%Y-%m-%d') 
         for i in range((end_date - start_date).days + 1)]
    ).map(lambda x: (x,)).toDF(['date_str'])
    
    dim_date = date_range.select(
        col('date_str').cast('date').alias('date_key'),
        year(col('date_str').cast('date')).alias('year'),
        month(col('date_str').cast('date')).alias('month'),
        dayofmonth(col('date_str').cast('date')).alias('day'),
        weekofyear(col('date_str').cast('date')).alias('week'),
        dayofweek(col('date_str').cast('date')).alias('day_of_week'),
        date_format(col('date_str').cast('date'), 'EEEE').alias('day_name')
    )
    
    log_info(f"DimDate created with {dim_date.count()} records")
    return dim_date

# ============================================================================
# FACT TABLES
# ============================================================================

def create_fact_media_daily():
    """Create FactMediaDaily fact table"""
    log_info("Creating FactMediaDaily...")
    
    # Read stats by date
    stats_path = f'{RAW_PATH}/media_stats_by_date/dt={EXECUTION_DATE}/'
    df = read_json_from_s3(stats_path)
    
    if df is None or df.count() == 0:
        log_info("No media stats found")
        return None
    
    # Transform: Flatten nested JSON and calculate metrics
    fact_daily = df.select(
        col('media_id').alias('media_id'),
        col('date').cast('date').alias('date_key'),
        col('pageLoads').cast('long').alias('page_loads'),
        col('uniqueVisitors').cast('long').alias('unique_visitors'),
        col('plays').cast('long').alias('plays'),
        when(col('pageLoads') > 0, col('plays') / col('pageLoads')).otherwise(0).alias('play_rate'),
        col('timePlayed').cast('long').alias('time_played_seconds'),
        col('engagement').cast('double').alias('avg_engagement_pct'),
        col('form_submissions').cast('long').alias('form_submissions'),
        col('cta_clicks').cast('long').alias('cta_clicks'),
        current_timestamp().alias('load_timestamp')
    ).filter(col('media_id').isNotNull())
    
    log_info(f"FactMediaDaily created with {fact_daily.count()} records")
    return fact_daily

def create_fact_visitor_engagement():
    """Create FactVisitorEngagement fact table"""
    log_info("Creating FactVisitorEngagement...")
    
    # Read visitors data
    visitors_path = f'{RAW_PATH}/visitors/dt={EXECUTION_DATE}/'
    df = read_json_from_s3(visitors_path)
    
    if df is None or df.count() == 0:
        log_info("No visitor data found")
        return None
    
    # Transform: Select and aggregate visitor engagement
    fact_visitor = df.select(
        col('id').alias('visitor_id'),
        col('media_id').alias('media_id'),
        col('created_at').cast('date').alias('date_key'),
        col('plays').cast('long').alias('plays'),
        col('timePlayed').cast('long').alias('watch_time_seconds'),
        col('load').cast('boolean').alias('completed'),
        col('rewatched').cast('long').alias('replay_count'),
        col('firstPlay').cast('timestamp').alias('first_play_at'),
        col('lastPlay').cast('timestamp').alias('last_play_at'),
        current_timestamp().alias('load_timestamp')
    ).filter(col('visitor_id').isNotNull())
    
    log_info(f"FactVisitorEngagement created with {fact_visitor.count()} records")
    return fact_visitor

# ============================================================================
# MAIN ETL PROCESS
# ============================================================================

try:
    log_info("Starting Glue ETL job")
    log_info(f"Execution Date: {EXECUTION_DATE}")
    
    # Create dimensions
    dim_media = create_dim_media()
    dim_date = create_dim_date()
    
    # Create facts
    fact_daily = create_fact_media_daily()
    fact_visitor = create_fact_visitor_engagement()
    
    # Write to S3 curated zone
    if dim_media is not None:
        write_parquet_to_s3(
            dim_media,
            f'{CURATED_PATH}/dim_media/dt={EXECUTION_DATE}'
        )
    
    if dim_date is not None:
        write_parquet_to_s3(
            dim_date,
            f'{CURATED_PATH}/dim_date/dt={EXECUTION_DATE}'
        )
    
    if fact_daily is not None:
        write_parquet_to_s3(
            fact_daily,
            f'{CURATED_PATH}/fact_media_daily/dt={EXECUTION_DATE}'
        )
    
    if fact_visitor is not None:
        write_parquet_to_s3(
            fact_visitor,
            f'{CURATED_PATH}/fact_visitor_engagement/dt={EXECUTION_DATE}'
        )
    
    log_info("ETL job completed successfully")
    job.commit()

except Exception as e:
    log_info(f"ERROR in ETL job: {str(e)}")
    import traceback
    log_info(traceback.format_exc())
    raise e
