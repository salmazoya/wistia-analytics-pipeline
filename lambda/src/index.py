"""
Wistia Analytics Data Ingestion Lambda Function
Fetches data from Wistia APIs and writes to S3
"""

import json
import boto3
import requests
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import os
from functools import lru_cache

# Initialize AWS clients
s3_client = boto3.client('s3')
dynamodb_client = boto3.client('dynamodb')
ssm_client = boto3.client('ssm')
secrets_client = boto3.client('secretsmanager')

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Constants
MAX_RETRIES = 3
RETRY_DELAY = 1  # seconds


class WistiaIngestionError(Exception):
    """Custom exception for Wistia ingestion errors"""
    pass


class ParameterStore:
    """Wrapper for SSM Parameter Store operations"""
    
    @staticmethod
    @lru_cache(maxsize=10)
    def get_parameter(param_name: str) -> str:
        """Get parameter from Parameter Store with caching"""
        try:
            response = ssm_client.get_parameter(Name=param_name, WithDecryption=True)
            return response['Parameter']['Value']
        except Exception as e:
            logger.error(f"Failed to get parameter {param_name}: {str(e)}")
            raise WistiaIngestionError(f"Parameter retrieval failed: {param_name}")

    @staticmethod
    def get_parameters_dict(param_prefix: str) -> Dict[str, str]:
        """Get all parameters by path prefix"""
        try:
            response = ssm_client.get_parameters_by_path(
                Path=param_prefix,
                Recursive=True,
                WithDecryption=True
            )
            
            params = {}
            for param in response['Parameters']:
                # Extract key from full path (e.g., /wistia/media-ids -> media-ids)
                key = param['Name'].split('/')[-1]
                params[key] = param['Value']
            
            return params
        except Exception as e:
            logger.error(f"Failed to get parameters by path {param_prefix}: {str(e)}")
            raise WistiaIngestionError(f"Parameter retrieval failed for prefix: {param_prefix}")


class SecretsManager:
    """Wrapper for Secrets Manager operations"""
    
    @staticmethod
    @lru_cache(maxsize=5)
    def get_secret(secret_name: str) -> str:
        """Get secret from Secrets Manager with caching"""
        try:
            response = secrets_client.get_secret_value(SecretId=secret_name)
            
            if 'SecretString' in response:
                return response['SecretString']
            else:
                logger.error(f"Secret {secret_name} is binary, not supported")
                raise WistiaIngestionError(f"Binary secret not supported: {secret_name}")
        except Exception as e:
            logger.error(f"Failed to get secret {secret_name}: {str(e)}")
            raise WistiaIngestionError(f"Secret retrieval failed: {secret_name}")


class WistiaAPIClient:
    """Client for Wistia API calls"""
    
    def __init__(self, base_url: str, api_token: str, per_page: int = 100, max_retries: int = MAX_RETRIES):
        self.base_url = base_url
        self.api_token = api_token
        self.per_page = per_page
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {api_token}',
            'Accept': 'application/json'
        })

    def _request_with_retry(self, method: str, url: str, **kwargs) -> requests.Response:
        """Make HTTP request with exponential backoff retry logic"""
        for attempt in range(self.max_retries):
            try:
                response = self.session.request(method, url, timeout=30, **kwargs)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                if attempt == self.max_retries - 1:
                    logger.error(f"Failed after {self.max_retries} retries: {str(e)}")
                    raise WistiaIngestionError(f"API request failed: {str(e)}")
                
                wait_time = RETRY_DELAY * (2 ** attempt)
                logger.warning(f"Attempt {attempt + 1} failed, retrying in {wait_time}s: {str(e)}")
                import time
                time.sleep(wait_time)

    def get_media_metadata(self) -> List[Dict[str, Any]]:
        """Fetch all media metadata"""
        logger.info("Fetching media metadata from Wistia API")
        all_media = []
        page = 1
        
        while True:
            url = f"{self.base_url}/medias"
            params = {
                'page': page,
                'per_page': self.per_page,
                'sort_by': 'updated',
                'sort_direction': 'desc'
            }
            
            response = self._request_with_retry('GET', url, params=params)
            media_list = response.json()
            
            if not media_list:
                logger.info(f"No more media to fetch at page {page}")
                break
            
            all_media.extend(media_list)
            logger.info(f"Fetched {len(media_list)} media from page {page}")
            page += 1
        
        logger.info(f"Total media fetched: {len(all_media)}")
        return all_media

    def get_media_stats_by_date(self, media_id: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Fetch media statistics by date"""
        logger.info(f"Fetching stats for media {media_id} from {start_date} to {end_date}")
        
        url = f"{self.base_url}/stats/medias/{media_id}/by_date"
        params = {
            'start_date': start_date,
            'end_date': end_date
        }
        
        response = self._request_with_retry('GET', url, params=params)
        stats = response.json()
        
        logger.info(f"Fetched {len(stats)} daily stat records for media {media_id}")
        return stats

    def get_visitors(self, media_id: str) -> List[Dict[str, Any]]:
        """Fetch visitor data for a media"""
        logger.info(f"Fetching visitors for media {media_id}")
        all_visitors = []
        page = 1
        
        while True:
            url = f"{self.base_url}/stats/visitors"
            params = {
                'media_id': media_id,
                'page': page,
                'per_page': self.per_page,
                'sort_by': 'created_at',
                'sort_direction': 'desc'
            }
            
            response = self._request_with_retry('GET', url, params=params)
            visitors = response.json()
            
            if not visitors:
                logger.info(f"No more visitors to fetch for media {media_id} at page {page}")
                break
            
            all_visitors.extend(visitors)
            logger.info(f"Fetched {len(visitors)} visitors from page {page}")
            page += 1
        
        logger.info(f"Total visitors fetched for {media_id}: {len(all_visitors)}")
        return all_visitors

    def get_events(self, media_id: str) -> List[Dict[str, Any]]:
        """Fetch event data for a media"""
        logger.info(f"Fetching events for media {media_id}")
        all_events = []
        page = 1
        
        while True:
            url = f"{self.base_url}/stats/events"
            params = {
                'media_id': media_id,
                'page': page,
                'per_page': self.per_page,
                'sort_by': 'occurred_at',
                'sort_direction': 'desc'
            }
            
            response = self._request_with_retry('GET', url, params=params)
            events = response.json()
            
            if not events:
                logger.info(f"No more events to fetch for media {media_id} at page {page}")
                break
            
            all_events.extend(events)
            logger.info(f"Fetched {len(events)} events from page {page}")
            page += 1
        
        logger.info(f"Total events fetched for {media_id}: {len(all_events)}")
        return all_events


class S3DataLake:
    """Wrapper for S3 data lake operations"""
    
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name

    def write_json(self, key: str, data: Any) -> bool:
        """Write JSON data to S3"""
        try:
            json_data = json.dumps(data, default=str)
            s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=json_data,
                ContentType='application/json'
            )
            logger.info(f"Successfully wrote to S3: s3://{self.bucket_name}/{key}")
            return True
        except Exception as e:
            logger.error(f"Failed to write to S3: {str(e)}")
            raise WistiaIngestionError(f"S3 write failed: {str(e)}")

    def write_batch(self, prefix: str, data_list: List[Dict], batch_size: int = 100) -> int:
        """Write data in batches to S3"""
        total_written = 0
        
        for i in range(0, len(data_list), batch_size):
            batch = data_list[i:i + batch_size]
            batch_key = f"{prefix}/batch_{i//batch_size}.json"
            self.write_json(batch_key, batch)
            total_written += len(batch)
        
        logger.info(f"Wrote {total_written} records to S3 with prefix {prefix}")
        return total_written


class WatermarkTracker:
    """Track ingestion watermarks in DynamoDB"""
    
    def __init__(self, table_name: str):
        self.table_name = table_name

    def get_watermark(self, media_id: str) -> Optional[str]:
        """Get last processed date for a media"""
        try:
            response = dynamodb_client.get_item(
                TableName=self.table_name,
                Key={'media_id': {'S': media_id}}
            )
            
            if 'Item' in response:
                last_date = response['Item'].get('last_processed_date', {}).get('S')
                logger.info(f"Watermark for {media_id}: {last_date}")
                return last_date
            
            logger.info(f"No watermark found for {media_id}")
            return None
        except Exception as e:
            logger.error(f"Failed to get watermark for {media_id}: {str(e)}")
            return None

    def update_watermark(self, media_id: str, last_date: str, status: str = "SUCCESS", error_message: str = None) -> bool:
        """Update watermark for a media"""
        try:
            timestamp = int(datetime.now().timestamp())
            
            item = {
                'media_id': {'S': media_id},
                'last_processed_date': {'S': last_date},
                'last_processed_timestamp': {'N': str(timestamp)},
                'status': {'S': status},
                'updated_at': {'N': str(timestamp)}
            }
            
            if error_message:
                item['error_message'] = {'S': error_message}
            
            # Increment run_count
            dynamodb_client.update_item(
                TableName=self.table_name,
                Key={'media_id': {'S': media_id}},
                UpdateExpression='SET last_processed_date = :lpd, last_processed_timestamp = :lpt, #s = :status, updated_at = :ua ADD run_count :rc',
                ExpressionAttributeNames={'#s': 'status'},
                ExpressionAttributeValues={
                    ':lpd': {'S': last_date},
                    ':lpt': {'N': str(timestamp)},
                    ':status': {'S': status},
                    ':ua': {'N': str(timestamp)},
                    ':rc': {'N': '1'}
                }
            )
            
            logger.info(f"Updated watermark for {media_id}: {last_date} ({status})")
            return True
        except Exception as e:
            logger.error(f"Failed to update watermark for {media_id}: {str(e)}")
            return False


def parse_media_ids(media_ids_str: str) -> List[str]:
    """Parse media IDs from JSON string"""
    try:
        # Try to parse as JSON array
        media_ids = json.loads(media_ids_str)
        if isinstance(media_ids, list):
            return media_ids
        return [media_ids]
    except:
        # If not JSON, treat as comma-separated or single value
        if ',' in media_ids_str:
            return [m.strip() for m in media_ids_str.split(',')]
        return [media_ids_str.strip()]


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for Wistia data ingestion
    
    Environment variables required:
    - WISTIA_SECRET_NAME: Name of secret containing API token
    - MEDIA_IDS_PARAM: Parameter store name for media IDs
    - BASE_URL_PARAM: Parameter store name for API base URL
    - PER_PAGE_PARAM: Parameter store name for pagination size
    - S3_BUCKET: S3 bucket name for data lake
    - DYNAMODB_WATERMARKS_TABLE: DynamoDB table for watermarks
    - EXECUTION_DATE: Date to process (optional, defaults to today)
    """
    
    start_time = datetime.now()
    execution_stats = {
        'start_time': start_time.isoformat(),
        'status': 'FAILED',
        'media_processed': 0,
        'records_ingested': 0,
        'errors': []
    }
    
    try:
        # Load configuration
        logger.info("Loading configuration from Parameter Store and Secrets Manager")
        
        params = ParameterStore.get_parameters_dict('/wistia')
        api_token = SecretsManager.get_secret(os.environ.get('WISTIA_SECRET_NAME', 'wistia-api-token'))
        
        # Parse configuration
        media_ids = parse_media_ids(params.get('media-ids', '[]'))
        base_url = params.get('base-url', 'https://api.wistia.com/v1')
        per_page = int(params.get('per-page', '100'))
        s3_bucket = os.environ.get('S3_BUCKET', 'wistia-analytics')
        watermarks_table = os.environ.get('DYNAMODB_WATERMARKS_TABLE', 'wistia_watermarks')
        
        # Get execution date (default: yesterday)
        execution_date = event.get('execution_date', (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'))
        
        logger.info(f"Configuration loaded: {len(media_ids)} media IDs, base_url={base_url}")
        
        # Initialize clients
        wistia_client = WistiaAPIClient(base_url, api_token, per_page)
        data_lake = S3DataLake(s3_bucket)
        watermark_tracker = WatermarkTracker(watermarks_table)
        
        # Step 1: Fetch and store media metadata
        logger.info("=== STEP 1: Fetching Media Metadata ===")
        try:
            all_media = wistia_client.get_media_metadata()
            
            # Filter to only our media IDs
            filtered_media = [m for m in all_media if m.get('id') in media_ids or m.get('hashed_id') in media_ids]
            
            if filtered_media:
                data_lake.write_json(
                    f"raw/media_metadata/dt={execution_date}/media_metadata.json",
                    filtered_media
                )
                logger.info(f"Stored {len(filtered_media)} media metadata records")
                execution_stats['records_ingested'] += len(filtered_media)
        except WistiaIngestionError as e:
            logger.error(f"Failed to fetch media metadata: {str(e)}")
            execution_stats['errors'].append(str(e))
        
        # Step 2: Fetch and store stats by date
        logger.info("=== STEP 2: Fetching Media Stats by Date ===")
        for media_id in media_ids:
            try:
                # Get last processed date
                last_processed = watermark_tracker.get_watermark(media_id)
                
                if last_processed:
                    start_date = (datetime.strptime(last_processed, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
                else:
                    # Default to 90 days back if no watermark
                    start_date = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
                
                end_date = execution_date
                
                logger.info(f"Fetching stats for {media_id} from {start_date} to {end_date}")
                
                stats = wistia_client.get_media_stats_by_date(media_id, start_date, end_date)
                
                if stats:
                    data_lake.write_json(
                        f"raw/media_stats_by_date/dt={execution_date}/media_id={media_id}_stats.json",
                        stats
                    )
                    logger.info(f"Stored {len(stats)} stat records for {media_id}")
                    execution_stats['records_ingested'] += len(stats)
                    
                    # Update watermark
                    watermark_tracker.update_watermark(media_id, end_date, "SUCCESS")
                else:
                    logger.warning(f"No stats found for {media_id}")
                    watermark_tracker.update_watermark(media_id, end_date, "SUCCESS")
                
                execution_stats['media_processed'] += 1
                
            except WistiaIngestionError as e:
                logger.error(f"Failed to fetch stats for {media_id}: {str(e)}")
                execution_stats['errors'].append(f"Media {media_id}: {str(e)}")
                watermark_tracker.update_watermark(media_id, execution_date, "FAILED", str(e))
        
        # Step 3: Fetch and store visitors
        logger.info("=== STEP 3: Fetching Visitors ===")
        for media_id in media_ids:
            try:
                visitors = wistia_client.get_visitors(media_id)
                
                if visitors:
                    data_lake.write_json(
                        f"raw/visitors/dt={execution_date}/media_id={media_id}_visitors.json",
                        visitors
                    )
                    logger.info(f"Stored {len(visitors)} visitor records for {media_id}")
                    execution_stats['records_ingested'] += len(visitors)
                else:
                    logger.warning(f"No visitors found for {media_id}")
                
            except WistiaIngestionError as e:
                logger.error(f"Failed to fetch visitors for {media_id}: {str(e)}")
                execution_stats['errors'].append(f"Visitors for {media_id}: {str(e)}")
        
        # Step 4: Fetch and store events
        logger.info("=== STEP 4: Fetching Events ===")
        for media_id in media_ids:
            try:
                events = wistia_client.get_events(media_id)
                
                if events:
                    data_lake.write_json(
                        f"raw/events/dt={execution_date}/media_id={media_id}_events.json",
                        events
                    )
                    logger.info(f"Stored {len(events)} event records for {media_id}")
                    execution_stats['records_ingested'] += len(events)
                else:
                    logger.warning(f"No events found for {media_id}")
                
            except WistiaIngestionError as e:
                logger.error(f"Failed to fetch events for {media_id}: {str(e)}")
                execution_stats['errors'].append(f"Events for {media_id}: {str(e)}")
        
        # Set final status
        execution_stats['status'] = 'SUCCESS' if not execution_stats['errors'] else 'PARTIAL_SUCCESS'
        execution_stats['end_time'] = datetime.now().isoformat()
        execution_stats['duration_seconds'] = (datetime.now() - start_time).total_seconds()
        
        logger.info(f"Ingestion complete. Status: {execution_stats['status']}")
        logger.info(f"Records ingested: {execution_stats['records_ingested']}")
        logger.info(f"Media processed: {execution_stats['media_processed']}")
        
        return {
            'statusCode': 200,
            'body': json.dumps(execution_stats)
        }
    
    except Exception as e:
        logger.error(f"Unexpected error in Lambda handler: {str(e)}")
        execution_stats['status'] = 'FAILED'
        execution_stats['errors'].append(str(e))
        execution_stats['end_time'] = datetime.now().isoformat()
        execution_stats['duration_seconds'] = (datetime.now() - start_time).total_seconds()
        
        return {
            'statusCode': 500,
            'body': json.dumps(execution_stats)
        }


if __name__ == '__main__':
    # For local testing
    test_event = {
        'execution_date': (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    }
    result = lambda_handler(test_event, None)
    print(json.dumps(json.loads(result['body']), indent=2))
