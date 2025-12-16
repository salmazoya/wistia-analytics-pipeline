"""
Wistia Analytics Data Ingestion Lambda Function
Fetches data from Wistia APIs and writes to S3
Updated to read API token from AWS Secrets Manager
"""

import json
import boto3
import requests
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

# Initialize AWS clients
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
secrets_client = boto3.client('secretsmanager')
ssm_client = boto3.client('ssm')

# Environment variables
BUCKET_NAME = 'wistia-analytics'
DYNAMODB_TABLE = 'wistia_run_status'
SECRETS_NAME = 'wistia-api-token'  # Secrets Manager secret name

# Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_wistia_token() -> str:
    """
    Retrieve Wistia API token from AWS Secrets Manager
    """
    try:
        logger.info(f"Fetching Wistia API token from Secrets Manager: {SECRETS_NAME}")
        
        response = secrets_client.get_secret_value(SecretId=SECRETS_NAME)
        
        # Handle different secret formats
        if 'SecretString' in response:
            secret = json.loads(response['SecretString'])
            # Check if it's a JSON object with 'api_token' key
            if isinstance(secret, dict) and 'api_token' in secret:
                token = secret['api_token']
            # Or if it's just the token string
            elif isinstance(secret, str):
                token = secret
            else:
                token = str(secret)
        else:
            token = response['SecretBinary']
        
        logger.info("✅ Successfully retrieved Wistia API token from Secrets Manager")
        return token.strip()
        
    except Exception as e:
        logger.error(f"❌ Failed to retrieve Wistia API token from Secrets Manager: {str(e)}")
        raise


def get_wistia_medias(api_token: str) -> List[Dict[str, Any]]:
    """
    Fetch all media from Wistia account
    """
    try:
        logger.info("Fetching media list from Wistia API...")
        
        url = "https://api.wistia.com/v1/medias.json"
        params = {'api_password': api_token}
        
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        medias = response.json()
        logger.info(f"✅ Retrieved {len(medias)} medias from Wistia")
        
        return medias
        
    except Exception as e:
        logger.error(f"❌ Failed to fetch media from Wistia: {str(e)}")
        raise


def get_media_stats(media_id: str, api_token: str) -> Dict[str, Any]:
    """
    Fetch engagement statistics for a specific media
    """
    try:
        url = f"https://api.wistia.com/v1/medias/{media_id}/stats.json"
        params = {'api_password': api_token}
        
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        return response.json()
        
    except Exception as e:
        logger.warning(f"⚠️ Failed to fetch stats for media {media_id}: {str(e)}")
        return {}


def save_to_s3(data: Dict[str, Any], s3_path: str) -> None:
    """
    Save data to S3
    """
    try:
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_path,
            Body=json.dumps(data, indent=2),
            ContentType='application/json'
        )
        logger.info(f"✅ Saved data to s3://{BUCKET_NAME}/{s3_path}")
        
    except Exception as e:
        logger.error(f"❌ Failed to save to S3: {str(e)}")
        raise


def log_run_status(status: Dict[str, Any]) -> None:
    """
    Log run status to DynamoDB
    """
    try:
        table = dynamodb.Table(DYNAMODB_TABLE)
        table.put_item(Item=status)
        logger.info(f"✅ Logged run status to DynamoDB")
        
    except Exception as e:
        logger.warning(f"⚠️ Failed to log to DynamoDB: {str(e)}")


def lambda_handler(event, context):
    """
    Main Lambda handler function
    """
    
    start_time = datetime.utcnow()
    execution_date = event.get('execution_date', start_time.strftime('%Y-%m-%d'))
    
    status = {
        'execution_date': execution_date,
        'start_time': start_time.isoformat(),
        'status': 'IN_PROGRESS',
        'media_processed': 0,
        'records_ingested': 0,
        'errors': []
    }
    
    try:
        logger.info(f"🚀 Starting Wistia ingestion for {execution_date}")
        
        # Step 1: Get Wistia API token from Secrets Manager
        logger.info("Step 1: Retrieving API credentials...")
        try:
            api_token = get_wistia_token()
        except Exception as e:
            status['errors'].append(f"API token retrieval failed: {str(e)}")
            status['status'] = 'FAILED'
            end_time = datetime.utcnow()
            status['end_time'] = end_time.isoformat()
            status['duration_seconds'] = (end_time - start_time).total_seconds()
            log_run_status(status)
            
            return {
                'statusCode': 500,
                'body': json.dumps(status)
            }
        
        # Step 2: Fetch media list
        logger.info("Step 2: Fetching media list...")
        try:
            medias = get_wistia_medias(api_token)
        except Exception as e:
            status['errors'].append(f"Media fetch failed: {str(e)}")
            status['status'] = 'FAILED'
            end_time = datetime.utcnow()
            status['end_time'] = end_time.isoformat()
            status['duration_seconds'] = (end_time - start_time).total_seconds()
            log_run_status(status)
            
            return {
                'statusCode': 500,
                'body': json.dumps(status)
            }
        
        # Step 3: Process each media
        logger.info(f"Step 3: Processing {len(medias)} medias...")
        
        for media in medias:
            try:
                media_id = media.get('id', 'unknown')
                media_name = media.get('name', 'unknown')
                
                logger.info(f"Processing media: {media_name} ({media_id})")
                
                # Get stats for this media
                stats = get_media_stats(media_id, api_token)
                
                # Combine media info with stats
                media_data = {
                    'media_id': media_id,
                    'name': media_name,
                    'media_info': media,
                    'stats': stats,
                    'ingestion_timestamp': datetime.utcnow().isoformat()
                }
                
                # Save to S3
                s3_path = f"raw/wistia/execution_date={execution_date}/media_id={media_id}/data.json"
                save_to_s3(media_data, s3_path)
                
                status['media_processed'] += 1
                status['records_ingested'] += 1
                
            except Exception as e:
                logger.error(f"❌ Error processing media {media_id}: {str(e)}")
                status['errors'].append(f"Media processing error: {str(e)}")
                continue
        
        # Step 4: Success
        status['status'] = 'SUCCESS'
        logger.info(f"✅ Ingestion completed successfully")
        
    except Exception as e:
        logger.error(f"❌ Unexpected error in Lambda: {str(e)}")
        status['errors'].append(f"Unexpected error: {str(e)}")
        status['status'] = 'FAILED'
    
    finally:
        # Log final status
        end_time = datetime.utcnow()
        status['end_time'] = end_time.isoformat()
        status['duration_seconds'] = (end_time - start_time).total_seconds()
        
        try:
            log_run_status(status)
        except Exception as e:
            logger.warning(f"⚠️ Could not log final status: {str(e)}")
    
    # Return response
    return {
        'statusCode': 200 if status['status'] == 'SUCCESS' else 500,
        'body': json.dumps(status)
    }