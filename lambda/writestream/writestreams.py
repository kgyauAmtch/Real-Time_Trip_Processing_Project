import json
import boto3
import logging
import base64
from botocore.exceptions import ClientError
from datetime import datetime
from decimal import Decimal
import math

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
table_name = 'bolt'
table = dynamodb.Table(table_name)

# S3 setup for rejected rows
s3 = boto3.client('s3')
REJECTED_BUCKET = 'your-rejected-records-bucket'  

# Required columns for validation
TRIP_START_COLUMNS = {
    'trip_id': str,
    'pickup_location_id': int,
    'dropoff_location_id': int,
    'vendor_id': int,
    'pickup_datetime': str,
    'estimated_dropoff_datetime': str,
    'estimated_fare_amount': float
}

TRIP_END_COLUMNS = {
    'trip_id': str,
    'dropoff_datetime': str,
    'rate_code': float,
    'passenger_count': float,
    'trip_distance': float,
    'fare_amount': float,
    'tip_amount': float,
    'payment_type': float,
    'trip_type': float
}

def sanitize_numeric_value(val):
    """
    Converts val to Decimal if valid number.
    Returns None if val is NaN or Infinity.
    Raises ValueError if val cannot be converted to float.
    """
    try:
        fval = float(val)
        if math.isnan(fval) or math.isinf(fval):
            logger.warning(f"Numeric value {val} is NaN or Infinity, replacing with None")
            return None
        return Decimal(str(fval))
    except (ValueError, TypeError) as e:
        raise ValueError(f"Invalid numeric value: {val} ({e})")

def validate_columns(data, required_schema):
    for col, col_type in required_schema.items():
        if col not in data:
            raise ValueError(f"Missing required column: {col}")
        val = data[col]
        if col_type == float:
            try:
                float(val)
            except (ValueError, TypeError):
                raise ValueError(f"Column {col} must be a number, got {type(val).__name__}")
        elif col_type == int:
            try:
                int(float(val))
            except (ValueError, TypeError):
                raise ValueError(f"Column {col} must be an integer, got {type(val).__name__}")
        elif col_type == str:
            if not isinstance(val, str):
                raise ValueError(f"Column {col} must be a string, got {type(val).__name__}")
    return True

def parse_iso8601(dt_str):
    # Support both 'YYYY-MM-DDTHH:MM:SS' and 'YYYY-MM-DD HH:MM:SS' formats
    try:
        return datetime.fromisoformat(dt_str)
    except ValueError:
        # Try replacing space with 'T' if needed
        try:
            return datetime.fromisoformat(dt_str.replace(' ', 'T'))
        except Exception:
            raise ValueError(f"Invalid datetime format: {dt_str}. Expected ISO 8601.")

def extract_date_from_datetime_string(datetime_str):
    """
    Extracts the date part (YYYY-MM-DD) from a datetime string.
    Supports both 'YYYY-MM-DD HH:MM:SS' and 'YYYY-MM-DDTHH:MM:SS' formats.
    """
    if not datetime_str or not isinstance(datetime_str, str):
        raise ValueError("Invalid datetime string for date extraction.")
    # Extract date part before space or 'T'
    return datetime_str.split(' ')[0].split('T')[0]

def process_trip_event(event_data):
    trip_id = event_data.get('trip_id')
    if not trip_id:
        raise ValueError("Missing 'trip_id' in event data.")

    logger.info(f"Processing event for trip_id: {trip_id}")
    try:
        expression_values = {}
        # is_start_event: has pickup_location_id but no dropoff_datetime (yet)
        is_start_event = 'pickup_location_id' in event_data and 'dropoff_datetime' not in event_data
        # is_end_event: has dropoff_datetime
        is_end_event = 'dropoff_datetime' in event_data

        update_expression_parts = []
        update_expression = "SET "
        last_updated = None # This will be the timestamp for the current event's processing

        if is_start_event:
            logger.info(f"Identified as trip START event for trip_id: {trip_id}")
            validate_columns(event_data, TRIP_START_COLUMNS)

            # Use pickup_datetime as last_updated for trip start
            last_updated = parse_iso8601(event_data['pickup_datetime']).isoformat()
            
            expression_values.update({
                ':pickup_location_id': int(event_data['pickup_location_id']),
                ':dropoff_location_id': int(event_data['dropoff_location_id']),
                ':vendor_id': int(event_data['vendor_id']),
                ':pickup_datetime': event_data['pickup_datetime'],
                ':estimated_dropoff_datetime': event_data['estimated_dropoff_datetime'],
                ':estimated_fare_amount': sanitize_numeric_value(event_data['estimated_fare_amount']),
                ':last_updated': last_updated
            })

            update_expression_parts.extend([
                "pickup_location_id = :pickup_location_id",
                "dropoff_location_id = :dropoff_location_id",
                "vendor_id = :vendor_id",
                "pickup_datetime = :pickup_datetime",
                "estimated_dropoff_datetime = :estimated_dropoff_datetime",
                "estimated_fare_amount = :estimated_fare_amount",
                "last_updated = :last_updated"
            ])

        elif is_end_event:
            logger.info(f"Identified as trip END event for trip_id: {trip_id}")
            validate_columns(event_data, TRIP_END_COLUMNS)

            # Use dropoff_datetime as last_updated for trip end
            last_updated = parse_iso8601(event_data['dropoff_datetime']).isoformat()
            
            # Extract and assign trip_date from dropoff_datetime
            trip_date = extract_date_from_datetime_string(event_data['dropoff_datetime'])

            expression_values.update({
                ':dropoff_datetime': event_data['dropoff_datetime'],
                ':rate_code': sanitize_numeric_value(event_data['rate_code']),
                ':passenger_count': sanitize_numeric_value(event_data['passenger_count']),
                ':trip_distance': sanitize_numeric_value(event_data['trip_distance']),
                ':fare_amount': sanitize_numeric_value(event_data['fare_amount']),
                ':tip_amount': sanitize_numeric_value(event_data['tip_amount']),
                ':payment_type': sanitize_numeric_value(event_data['payment_type']),
                ':trip_type': sanitize_numeric_value(event_data['trip_type']),
                ':trip_date': trip_date, # This is the key change: storing as 'trip_date'
                ':last_updated': last_updated
            })

            update_expression_parts.extend([
                "dropoff_datetime = :dropoff_datetime",
                "rate_code = :rate_code",
                "passenger_count = :passenger_count",
                "trip_distance = :trip_distance",
                "fare_amount = :fare_amount",
                "tip_amount = :tip_amount",
                "payment_type = :payment_type",
                "trip_type = :trip_type",
                "trip_date = :trip_date", # This is the key change: updating 'trip_date'
                "last_updated = :last_updated"
            ])

        else:
            raise ValueError(f"Event for trip_id {trip_id} does not match expected schemas.")

        update_expression += ", ".join(update_expression_parts)

        # Perform the update with conditional check on last_updated
        response = table.update_item(
            Key={'trip_id': trip_id},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_values,
            ConditionExpression="attribute_not_exists(last_updated) OR last_updated < :last_updated",
            ReturnValues="ALL_NEW"
        )

        updated_attributes = response.get('Attributes', {})

        # Mark trip as completed if both pickup and dropoff datetime exist
        if updated_attributes.get('pickup_datetime') and updated_attributes.get('dropoff_datetime'):
            table.update_item(
                Key={'trip_id': trip_id},
                UpdateExpression="SET trip_completed = :true_val",
                ExpressionAttributeValues={':true_val': True}
            )
            updated_attributes['trip_completed'] = True
            logger.info(f"Marked trip_id {trip_id} as completed.")

        return {'trip_id': trip_id, 'status': 'updated', 'attributes': updated_attributes}

    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            logger.warning(f"Stale update ignored for trip_id {trip_id}.")
            return {'trip_id': trip_id, 'status': 'stale_update_ignored'}
        else:
            logger.error(f"DynamoDB error for trip_id {trip_id}: {e}")
            raise
    except Exception as ex:
        logger.error(f"Error processing trip_id {trip_id}: {ex}")
        raise

def lambda_handler(event, context):
    logger.info(f"Received Kinesis event with {len(event.get('Records', []))} records.")

    results = []
    errors = []
    rejected_records = []

    for record in event.get('Records', []):
        payload = None
        try:
            payload_base64 = record['kinesis']['data']
            payload = base64.b64decode(payload_base64).decode('utf-8')
            logger.info(f"Decoded payload: {payload}")

            data = json.loads(payload)
            result = process_trip_event(data)
            results.append(result)
        except Exception as e:
            error_details = {
                'error': str(e),
                'kinesis_sequence_number': record.get('kinesis', {}).get('sequenceNumber'),
                'kinesis_partition_key': record.get('kinesis', {}).get('partitionKey'),
                'payload_sample': payload[:200] if payload else 'N/A'
            }
            logger.error(f"Error processing record: {json.dumps(error_details)}")
            errors.append(error_details)
            # Add the full payload and error for S3
            rejected_records.append({
                'error': str(e),
                'kinesis_sequence_number': record.get('kinesis', {}).get('sequenceNumber'),
                'kinesis_partition_key': record.get('kinesis', {}).get('partitionKey'),
                'payload': payload if payload else 'N/A'
            })

    # Write all rejected records to S3 as a single JSON file, if any
    if rejected_records:
        s3_key = f"rejected/rejected_records_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}.json"
        try:
            s3.put_object(
                Bucket=REJECTED_BUCKET,
                Key=s3_key,
                Body=json.dumps(rejected_records, indent=2).encode('utf-8')
            )
            logger.info(f"Wrote {len(rejected_records)} rejected records to s3://{REJECTED_BUCKET}/{s3_key}")
        except Exception as s3e:
            logger.error(f"Failed to write rejected records to S3: {s3e}")

    return {
        'statusCode': 200,
        'body': json.dumps({
            'processed_records_count': len(results),
            'failed_records_count': len(errors),
            'results': results,
            'errors_summary': errors
        })
    }
